import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * 🌍 GLOBAL social signals (default)
 * GET /api/social-signals
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
    console.error("❌ Error fetching global social signals:", err);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

/**
 * 🧪 DEBUG: return everything (no geo logic)
 * GET /api/social-signals/all
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
    console.error("❌ Error fetching all social signals:", err);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

/**
 * 🧪 DEBUG: counts by source
 * GET /api/social-signals/counts
 */
router.get("/counts", async (_req, res) => {
  try {
    const db = getDB();
    const col = db.collection("social_signals");

    const total = await col.countDocuments({});
    const gdelt = await col.countDocuments({ source: "GDELT" });
    const newsapi = await col.countDocuments({ provider: "NewsAPI" });
    const debug = await col.countDocuments({ source: "DEBUG" });

    res.json({ total, gdelt, newsapi, debug });
  } catch (err) {
    console.error("❌ Error counting social signals:", err);
    res.status(500).json({ error: "Failed to count social signals" });
  }
});

/**
 * 🧪 DEBUG: test insert (proves Mongo write + TTL not nuking instantly)
 * GET /api/social-signals/test-insert
 */
router.get("/test-insert", async (_req, res) => {
  try {
    const db = getDB();
    const col = db.collection("social_signals");

    const now = new Date();
    const doc = {
      type: "debug",
      source: "DEBUG",
      provider: "DEBUG",
      url: `debug://${Date.now()}`,
      title: "Debug marker",
      description: "If you can see this, the API is writing to the same collection you're viewing.",
      publishedAt: now,
      createdAt: now,
      updatedAt: now,
      expires: new Date(Date.now() + 24 * 3600 * 1000), // +24h, should NOT disappear immediately
      geometry: { type: "Point", coordinates: [0, 0] },
      geometryMethod: "debug",
      domain: "debug.local",
      hazardLabel: "Debug",
    };

    const r = await col.insertOne(doc);

    const counts = {
      total: await col.countDocuments({}),
      debug: await col.countDocuments({ source: "DEBUG" }),
    };

    res.json({ ok: true, insertedId: r.insertedId, counts });
  } catch (err) {
    console.error("❌ Error inserting debug doc:", err);
    res.status(500).json({ error: "Failed to insert debug doc", detail: String(err?.message || err) });
  }
});

/**
 * 📍 NEARBY social signals (geo-based)
 * GET /api/social-signals/near?lat=..&lng=..&radius_km=..
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
              coordinates: [Number(lng), Number(lat)],
            },
            $maxDistance: Number(radius_km) * 1000,
          },
        },
      })
      .limit(500)
      .toArray();

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching nearby social signals:", err);
    res.status(500).json({ error: "Failed to fetch social signals" });
  }
});

export default router;
