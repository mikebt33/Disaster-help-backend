// server.js — Disaster Help backend (instrumented for pilot metrics)

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import cron from "node-cron";
import crypto from "crypto";

import { connectDB, getDB } from "./src/db.js";
import { runCleanup } from "./src/services/cleanupService.js";
import { pollCapFeeds } from "./src/services/capPoller.mjs";
import { pollNewsAPI } from "./src/services/socialNewsPoller.mjs";
import { pollGDELT } from "./src/services/gdeltPoller.mjs";
import { ensureIndexes } from "./src/db/indexes.mjs";

import helpRoutes from "./src/routes/helpRequests.js";
import offerRoutes from "./src/routes/offers.js";
import hazardRoutes from "./src/routes/hazards.js";
import alertRoutes from "./src/routes/alertsCap.js";
import socialRoutes from "./src/routes/socialSignals.js";
import userRoutes from "./src/routes/user.js";
import followRouter from "./src/routes/follow.js";

import "./src/services/notifications.js";
import "dotenv/config";

dotenv.config();

// ---------------------------------------------------------------------------
// 🧭 Express App Setup
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(cors());

// ---------------------------------------------------------------------------
// 🆔 Request ID middleware (correlation for logs & metrics)
// ---------------------------------------------------------------------------
app.use((req, _res, next) => {
  req.requestId =
    req.headers["x-request-id"] || crypto.randomUUID();
  next();
});

// ---------------------------------------------------------------------------
// 🔗 Database Connection & Index Setup
// ---------------------------------------------------------------------------
await connectDB();
const db = getDB();

// ---------------------------------------------------------------------------
// 📊 Performance logging (non-blocking, safe)
// ---------------------------------------------------------------------------
app.use((req, res, next) => {
  const start = Date.now();

  res.on("finish", async () => {
    try {
      await db.collection("performance_logs").insertOne({
        requestId: req.requestId,
        route: req.originalUrl,
        method: req.method,
        statusCode: res.statusCode,
        responseTimeMs: Date.now() - start,
        userAgent: req.headers["user-agent"] || null,
        timestamp: new Date(),
      });
    } catch (err) {
      console.error("⚠️ Performance log failed:", err.message);
    }
  });

  next();
});

// ---------------------------------------------------------------------------
// 📍 Core geospatial indexes
// ---------------------------------------------------------------------------
await Promise.all([
  db.collection("hazards").createIndex({ geometry: "2dsphere" }),
  db.collection("alerts_cap").createIndex({ geometry: "2dsphere" }),
  db.collection("help_requests").createIndex({ location: "2dsphere" }),
  db.collection("offer_help").createIndex({ location: "2dsphere" }),
  db.collection("sent_events").createIndex(
    { lastSentAt: 1 },
    { expireAfterSeconds: 120 }
  ),
]);
console.log("✅ Geospatial indexes ensured");

// ---------------------------------------------------------------------------
// ⏱️ TTL indexes (72 hours)
// ---------------------------------------------------------------------------
const ttlSeconds = 72 * 60 * 60;
await Promise.all([
  db.collection("hazards").createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: ttlSeconds }
  ),
  db.collection("help_requests").createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: ttlSeconds }
  ),
  db.collection("offer_help").createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: ttlSeconds }
  ),
]);
console.log(`⏱️ TTL indexes set — ${ttlSeconds / 3600} hours`);

// ---------------------------------------------------------------------------
// 📐 Schema validation — help_requests
// ---------------------------------------------------------------------------
try {
  await db.command({
    collMod: "help_requests",
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["location", "timestamp"],
        properties: {
          location: {
            bsonType: "object",
            required: ["type", "coordinates"],
            properties: {
              type: { enum: ["Point"] },
              coordinates: {
                bsonType: "array",
                items: [{ bsonType: "double" }],
                minItems: 2,
                maxItems: 2,
              },
            },
          },
          message: { bsonType: "string" },
          timestamp: { bsonType: "date" },
        },
      },
    },
    validationLevel: "moderate",
  });
  console.log("✅ Schema validation applied for help_requests.");
} catch (e) {
  console.warn("⚠️ help_requests validation skipped:", e.message);
}

// ---------------------------------------------------------------------------
// 📐 Schema validation — hazards
// ---------------------------------------------------------------------------
try {
  await db.command({
    collMod: "hazards",
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["geometry", "timestamp"],
        properties: {
          geometry: {
            bsonType: "object",
            required: ["type", "coordinates"],
            properties: {
              type: { enum: ["Point"] },
              coordinates: {
                bsonType: "array",
                items: [{ bsonType: "double" }],
                minItems: 2,
                maxItems: 2,
              },
            },
          },
          message: { bsonType: "string" },
          type: { bsonType: "string" },
          timestamp: { bsonType: "date" },
        },
      },
    },
    validationLevel: "moderate",
  });
  console.log("✅ Schema validation applied for hazards.");
} catch (e) {
  console.warn("⚠️ hazards validation skipped:", e.message);
}

// ---------------------------------------------------------------------------
// 📐 Schema validation — alerts_cap
// ---------------------------------------------------------------------------
try {
  await db.command({
    collMod: "alerts_cap",
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["geometry"],
        properties: {
          geometry: {
            bsonType: "object",
            required: ["type", "coordinates"],
            properties: {
              type: { enum: ["Point"] },
              coordinates: {
                bsonType: "array",
                items: [{ bsonType: "double" }, { bsonType: "double" }],
                minItems: 2,
                maxItems: 2,
              },
            },
          },
        },
      },
    },
    validationLevel: "moderate",
  });
  console.log("✅ Schema validation applied for alerts_cap.");
} catch (e) {
  console.warn("⚠️ alerts_cap validation skipped:", e.message);
}

// ---------------------------------------------------------------------------
// 📰 Social/news indexes
// ---------------------------------------------------------------------------
await ensureIndexes();
console.log("✅ Social/news indexes ensured");

// Optional CAP pruning
await db.collection("alerts_cap").deleteMany({
  sent: { $lt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) },
});

// ---------------------------------------------------------------------------
// 🧭 API Routes
// ---------------------------------------------------------------------------
app.use("/api/help-requests", helpRoutes);
app.use("/api/offers", offerRoutes);
app.use("/api/hazards", hazardRoutes);
app.use("/api/alerts-cap", alertRoutes);
app.use("/api/social-signals", socialRoutes);
app.use("/api/user", userRoutes);
app.use("/api", followRouter);

// ---------------------------------------------------------------------------
// ❤️ Health check
// ---------------------------------------------------------------------------
app.get("/healthz", async (_req, res) => {
  try {
    await db.command({ ping: 1 });
    res.status(200).json({ ok: true });
  } catch {
    res.status(500).json({ ok: false });
  }
});

// Base route
app.get("/", (_req, res) => {
  res.send("🌍 Disaster Help backend is running and connected to MongoDB!");
});

// ---------------------------------------------------------------------------
// ❌ Global error handler (logs to Mongo safely)
// ---------------------------------------------------------------------------
app.use(async (err, req, res, _next) => {
  console.error("🔥 Unhandled error:", err);

  try {
    await db.collection("error_logs").insertOne({
      requestId: req.requestId,
      message: err.message,
      stack: err.stack,
      route: req.originalUrl,
      method: req.method,
      timestamp: new Date(),
    });
  } catch (logErr) {
    console.error("⚠️ Error logging failed:", logErr.message);
  }

  res.status(500).json({ error: "Internal server error" });
});

// ---------------------------------------------------------------------------
// 🕓 Cron Jobs
// ---------------------------------------------------------------------------
cron.schedule("0 2 * * *", async () => {
  console.log("⏱️ Scheduled cleanup starting...");
  await runCleanup();
});

// ---------------------------------------------------------------------------
// 🚀 Initial pollers (non-blocking)
// ---------------------------------------------------------------------------
setTimeout(() => pollCapFeeds(), 5000);
setTimeout(() => pollNewsAPI(), 8000);
setTimeout(() => pollGDELT(), 10000);

// ---------------------------------------------------------------------------
// ⏱️ Recurring pollers
// ---------------------------------------------------------------------------
cron.schedule("*/5 * * * *", pollCapFeeds);
cron.schedule("*/15 * * * *", async () => {
  await pollNewsAPI();
  await pollGDELT();
});

// ---------------------------------------------------------------------------
// 🚀 Start Server
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () =>
  console.log(`🚀 Server running on port ${PORT}`)
);
