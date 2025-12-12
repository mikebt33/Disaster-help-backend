import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * 🌍 GLOBAL social signals (default)
 * Used for:
 *  - Initial map load
 *  - Zoomed-out views
 *  - Debug sanity checks
 *
 * Route:
 *   GET /api/social-signals
 */
router.get("/", async (_req, res) => {
  try {
    const db = getDB();

    const results = await db
      .collection("social_signals")
      .find({})
      .sort({ publishedAt: -1, createdAt: -1 })
      .limit(500)
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching global social signals:", err.message);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

/**
 * 🧪 DEBUG / EXPLICIT ALL
 * Same as `/` but kept separate so you always have
 * a no-geo, no-logic sanity endpoint.
 *
 * Route:
 *   GET /api/social-signals/all
 */
router.get("/all", async (_req, res) => {
  try {
    const db = getDB();

    const results = await db
      .collection("social_signals")
      .find({})
      .limit(1000)
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching all social signals:", err.message);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

/**
 * 📍 NEARBY social signals (geo-based)
 * Used when map is zoomed in
 *
 * Route:
 *   GET /api/social-signals/near?lat=..&lng=..&radius_km=..
 */
router.get("/near", async (req, res) => {
  const { lat, lng, radius_km = 10000 } = req.query;

  if (!lat || !lng) {
    return res.status(400).json({
      error: "Missing lat and lng parameters",
    });
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
              coordinates: [
                Number(lng),
                Number(lat),
              ],
            },
            $maxDistance: Number(radius_km) * 1000,
          },
        },
      })
      .limit(500)
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching nearby social signals:", err.message);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

export default router;
