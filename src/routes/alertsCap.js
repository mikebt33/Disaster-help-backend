import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * GET /api/alerts-cap
 * List recent CAP alerts
 */
router.get("/", async (req, res) => {
  try {
    const db = getDB();
    const alerts = db.collection("alerts_cap");

    const recent = await alerts
      .find({})
      .sort({ sent: -1 })
      .limit(50)
      .toArray();

    res.json({
      count: recent.length,
      alerts: recent
    });
  } catch (error) {
    console.error("Error fetching CAP alerts:", error);
    res.status(500).json({ error: "Failed to fetch CAP alerts." });
  }
});

/**
 * GET /api/alerts-cap/:id
 * Fetch a specific alert by its identifier
 */
router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const alerts = db.collection("alerts_cap");

    const alert = await alerts.findOne({ identifier: req.params.id });
    if (!alert) return res.status(404).json({ error: "Alert not found" });

    res.json(alert);
  } catch (error) {
    console.error("Error fetching alert by ID:", error);
    res.status(500).json({ error: "Failed to fetch alert." });
  }
});

export default router;
