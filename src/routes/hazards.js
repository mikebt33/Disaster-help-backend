import express from "express";
import { getDB } from "../db.js";

const router = express.Router();

/**
 * POST /api/hazards
 * Create a hazard report (Point or Polygon)
 */
router.post("/", async (req, res) => {
  try {
    const { type, description, geometry, severity, source } = req.body;

    if (!geometry || !geometry.type || !geometry.coordinates) {
      return res.status(400).json({
        error: "geometry (GeoJSON object with type and coordinates) is required."
      });
    }

    const db = getDB();
    const hazards = db.collection("hazards");

    const doc = {
      type: type || "hazard",
      description: description || "",
      severity: severity || "Unknown",
      source: source || "user",
      geometry, // must be valid GeoJSON (Point or Polygon)
      timestamp: new Date()
    };

    const result = await hazards.insertOne(doc);
    res.status(201).json({ id: result.insertedId, ...doc });
  } catch (error) {
    console.error("❌ Error creating hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards/near?lat=27.77&lng=-82.64&radius_km=5
 * Finds hazards near a coordinate or containing that coordinate.
 * Supports both Point and Polygon hazards.
 */
router.get("/near", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km || 5);

    if (isNaN(lat) || isNaN(lng)) {
      return res
        .status(400)
        .json({ error: "Valid lat and lng query parameters are required." });
    }

    const db = getDB();
    const hazards = db.collection("hazards");

    // 1️⃣ Find nearby point hazards within a radius
    const pointHazards = await hazards
      .find({
        "geometry.type": "Point",
        geometry: {
          $geoWithin: {
            $centerSphere: [[lng, lat], radiusKm / 6378.1] // Earth's radius in km
          }
        }
      })
      .toArray();

    // 2️⃣ Find polygon hazards that contain the coordinate
    const polygonHazards = await hazards
      .find({
        "geometry.type": "Polygon",
        geometry: {
          $geoIntersects: {
            $geometry: { type: "Point", coordinates: [lng, lat] }
          }
        }
      })
      .toArray();

    // Combine both sets
    const results = [...pointHazards, ...polygonHazards];

    res.json({
      count: results.length,
      location: { lat, lng, radius_km: radiusKm },
      hazards: results
    });
  } catch (error) {
    console.error("❌ Error fetching nearby hazards:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards
 * Optional: List all hazards (for admin/debugging)
 */
router.get("/", async (req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const all = await hazards.find({}).sort({ timestamp: -1 }).limit(100).toArray();
    res.json(all);
  } catch (error) {
    console.error("❌ Error listing hazards:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
