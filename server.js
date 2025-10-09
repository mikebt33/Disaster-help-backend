import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import cron from "node-cron";
import { connectDB } from "./db.js";
import { runCleanup } from "./services/cleanupService.js";

// API routes
import helpRoutes from "./routes/helpRequests.js";
import offerRoutes from "./routes/offers.js";
import hazardRoutes from "./routes/hazards.js";
import alertRoutes from "./routes/alertsCap.js";

// CAP Poller service
import { pollCapFeeds } from "./services/capPoller.js";


dotenv.config();
const app = express();
app.use(express.json());
app.use(cors());

// 🕓 Run cleanup once per day at 2 AM UTC
cron.schedule("0 2 * * *", async () => {
  console.log("⏱️ Scheduled cleanup starting...");
  await runCleanup();
});

// Connect to MongoDB Atlas
await connectDB();

// Register routes
app.use("/api/help-requests", helpRoutes);
app.use("/api/offers", offerRoutes);
app.use("/api/hazards", hazardRoutes);
app.use("/api/alerts-cap", alertRoutes);

// Base route
app.get("/", (req, res) => {
  res.send("🌍 Disaster Help backend is running and connected to MongoDB!");
});

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

// ==============================
// 🕑 CAP Poller Auto Scheduler
// ==============================

// Run the CAP feed poller immediately at startup
console.log("⏱️ Initial CAP feed poll on startup...");
await pollCapFeeds();

// Then schedule it to re-run every 5 minutes
cron.schedule("*/5 * * * *", async () => {
  console.log("⏱️ Scheduled CAP alert ingestion running...");
  await pollCapFeeds();
});
