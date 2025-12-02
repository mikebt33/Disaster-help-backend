// server.js — Disaster Help backend

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import cron from "node-cron";
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
// 🔗 Database Connection & Index Setup
// ---------------------------------------------------------------------------
await connectDB();
const db = getDB();

// 1️⃣ Core geospatial indexes
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

// 2️⃣ TTL (Time-To-Live) indexes (72 hours)
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
console.log(`⏱️ TTL indexes set — data expires after ${ttlSeconds / 3600} hours.`);

// 3️⃣ Schema validation — help_requests
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
  console.warn("⚠️ Schema validation skipped or already exists:", e.message);
}

// 4️⃣ Schema validation — hazards
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
  console.warn("⚠️ hazards schema validation skipped or failed:", e.message);
}

// 5️⃣ Social/news TTL + geo indexes
await ensureIndexes();
console.log("✅ Social/news indexes ensured");

// Optional: prune old CAP alerts older than 7 days
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

// Health check
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
// 🕓 Cron Jobs
// ---------------------------------------------------------------------------

// Daily cleanup (2AM UTC)
cron.schedule("0 2 * * *", async () => {
  console.log("⏱️ Scheduled cleanup starting...");
  await runCleanup();
});

// ---------------------------------------------------------------------------
// 🚫 REMOVE ALL top-level awaits for pollers
//    They were blocking Express from binding the port.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ✅ Initial pollers — run AFTER server starts (non-blocking)
// ---------------------------------------------------------------------------
setTimeout(() => {
  console.log("⏱️ Initial CAP feed poll (delayed)...");
  pollCapFeeds();
}, 5000);

setTimeout(() => {
  console.log("📰 Initial NewsAPI poll (delayed)...");
  pollNewsAPI();
}, 8000);

setTimeout(() => {
  console.log("🌎 Initial GDELT poll (delayed)...");
  pollGDELT();
}, 10000);

// ---------------------------------------------------------------------------
// ⏱️ Scheduled recurring pollers
// ---------------------------------------------------------------------------

// CAP poller every 5 minutes
cron.schedule("*/5 * * * *", async () => {
  console.log("⏱️ Scheduled CAP alert ingestion running...");
  await pollCapFeeds();
});

// News + GDELT every 15 minutes
cron.schedule("*/15 * * * *", async () => {
  console.log("📰 Scheduled NewsAPI polling running...");
  await pollNewsAPI();

  console.log("🌎 Scheduled GDELT polling running...");
  await pollGDELT();
});

// ---------------------------------------------------------------------------
// 🚀 Start Server
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
