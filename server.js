import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import cron from "node-cron";
import { connectDB, getDB } from "./src/db.js";
import { runCleanup } from "./src/services/cleanupService.js";
import { pollCapFeeds } from "./src/services/capPoller.mjs";
import helpRoutes from "./src/routes/helpRequests.js";
import offerRoutes from "./src/routes/offers.js";
import hazardRoutes from "./src/routes/hazards.js";
import alertRoutes from "./src/routes/alertsCap.js";

dotenv.config();
const app = express();
app.use(express.json());
app.use(cors());

// 🕓 Run cleanup once per day at 2 AM UTC
cron.schedule("0 2 * * *", async () => {
  console.log("⏱️ Scheduled cleanup starting...");
  await runCleanup();
});

// Connect to MongoDB and ensure indexes
await connectDB();
const db = getDB();
await Promise.all([
  db.collection("hazards").createIndex({ geometry: "2dsphere" }),
  db.collection("alerts_cap").createIndex({ geometry: "2dsphere" }),
]);
console.log("✅ Geospatial indexes ensured");

// Optional: remove CAP alerts older than 7 days
await db.collection("alerts_cap").deleteMany({
  sent: { $lt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) },
});

// Register routes
app.use("/api/help-requests", helpRoutes);
app.use("/api/offers", offerRoutes);
app.use("/api/hazards", hazardRoutes);
app.use("/api/alerts-cap", alertRoutes);

// 🩺 Health check
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

// Start server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

// ==============================
// 🕑 CAP Poller Scheduler
// ==============================
console.log("⏱️ Initial CAP feed poll on startup...");
await pollCapFeeds();

cron.schedule("*/5 * * * *", async () => {
  console.log("⏱️ Scheduled CAP alert ingestion running...");
  await pollCapFeeds();
});
