import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * 🌍 GLOBAL social signals (GDELT + News)
 * Used when map is zoomed out or on initial load
 */
router.get("/", async (_req, res) => {
  try {
    const db = getDB();
    const results = await db
      .collection("social_signals")
      .find({})
      .sort({ publishedAt: -1 })
      .limit(500)
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching global social signals:", err.message);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

/**
 * 📍 NEARBY social signals (used when zoomed in)
 */
router.get("/near", async (req, res) => {
  const { lat, lng, radius_km = 10000 } = req.query;

  if (!lat || !lng) {
    return res.status(400).json({ error: "Missing lat and lng parameters" });
  }

  try {
    const db = getDB();
    const results = await db
      .collection("social_signals")
      .find({
        geometry: {
          $nearSphere: {
            $geometry: {
              type: "Point",
              coordinates: [parseFloat(lng), parseFloat(lat)],
            },
            $maxDistance: parseFloat(radius_km) * 1000,
          },
        },
      })
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching nearby social signals:", err.message);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

export default router;
