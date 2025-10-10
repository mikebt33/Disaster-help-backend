import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/help-requests
 * Create a new help request
 */
router.post("/", async (req, res) => {
  try {
    const { user_id, type, message, lat, lng } = req.body;

    if (!lat || !lng) {
      return res
        .status(400)
        .json({ error: "Latitude (lat) and longitude (lng) are required." });
    }

    const db = getDB();
    const helpRequests = db.collection("help_requests");

    const doc = {
      user_id: user_id || null,
      type: type || "general",
      message: message || "",
      location: {
        type: "Point",
        coordinates: [parseFloat(lng), parseFloat(lat)],
      },
      status: "open",
      timestamp: new Date(),
    };

    const result = await helpRequests.insertOne(doc);
    res.status(201).json({ id: result.insertedId.toString(), ...doc });
  } catch (error) {
    console.error("❌ Error creating help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/help-requests/near?lat=27.77&lng=-82.64&radius_km=5
 * Returns all help requests within a certain radius
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
    const helpRequests = db.collection("help_requests");

    const results = await helpRequests
      .find({
        location: {
          $nearSphere: {
            $geometry: { type: "Point", coordinates: [lng, lat] },
            $maxDistance: radiusKm * 1000, // convert km → meters
          },
        },
      })
      .limit(100)
      .toArray();

    res.json(results.map((r) => ({ ...r, _id: r._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching nearby help requests:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/help-requests/:id
 * Fetch a single help request by ObjectId or string id
 */
router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;

    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await helpRequests.findOne(query);
    if (!doc)
      return res.status(404).json({ error: "Help request not found." });

    res.json({ ...doc, _id: doc._id.toString() });
  } catch (error) {
    console.error("❌ Error fetching help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
