import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * GET /api/alerts-cap
 * Returns recent CAP alerts that have valid geometry
 */
router.get("/", async (req, res) => {
  try {
    const db = getDB();
    const alerts = db.collection("alerts_cap");

    // ✅ Only fetch alerts that have a geometry field
    const recent = await alerts
      .find({ geometry: { $ne: null } })
      .sort({ sent: -1 })
      .limit(200)
      .toArray();

    res.json({
      count: recent.length,
      alerts: recent,
    });
  } catch (error) {
    console.error("Error fetching CAP alerts:", error);
    res.status(500).json({ error: "Failed to fetch CAP alerts." });
  }
});


/**
 * GET /api/alerts-cap/:id
 * Fetch a specific CAP alert by _id (ObjectId or string) or by CAP identifier.
 */
router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const alerts = db.collection("alerts_cap");
    const { id } = req.params;

    console.log(`[CAP] Lookup requested for ID: ${id}`);
    const { ObjectId } = await import("mongodb");

    let alert = null;

    // 1️⃣ Try native ObjectId lookup
    if (/^[0-9a-fA-F]{24}$/.test(id)) {
      try {
        const objId = new ObjectId(id);
        alert = await alerts.findOne({ _id: objId });
        if (alert) console.log(`[CAP] ✅ Found by ObjectId: ${id}`);
      } catch (err) {
        console.warn(`[CAP] ⚠️ ObjectId lookup failed: ${err.message}`);
      }
    }

    // 2️⃣ Fallback: if stored as string instead of ObjectId
    if (!alert) {
      alert = await alerts.findOne({ _id: id });
      if (alert) console.log(`[CAP] ✅ Found by string _id: ${id}`);
    }

    // 3️⃣ Fallback: match by CAP identifier field
    if (!alert) {
      alert = await alerts.findOne({ identifier: id });
      if (alert) console.log(`[CAP] ✅ Found by identifier: ${id}`);
    }

    // 4️⃣ Still not found
    if (!alert) {
      console.warn(`[CAP] ❌ Alert not found for: ${id}`);
      return res.status(404).json({ error: "Alert not found" });
    }

    // ✅ Success
    res.json(alert);
  } catch (error) {
    console.error("Error fetching CAP alert:", error);
    res.status(500).json({ error: "Failed to fetch CAP alert." });
  }
});

export default router;
