import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/offers
 * Creates a new offer to help
 */
router.post("/", async (req, res) => {
  try {
    const { user_id, capabilities, message, lat, lng, available_until } = req.body;

    if (!lat || !lng) {
      return res
        .status(400)
        .json({ error: "Latitude (lat) and longitude (lng) are required." });
    }

    const db = getDB();
    const offers = db.collection("offer_help");

    const doc = {
      user_id: user_id || null,
      capabilities: capabilities || [],
      message: message || "",
      location: {
        type: "Point",
        coordinates: [parseFloat(lng), parseFloat(lat)],
      },
      available_until: available_until ? new Date(available_until) : null,
      timestamp: new Date(),
    };

    const result = await offers.insertOne(doc);
    res.status(201).json({ id: result.insertedId.toString(), ...doc });
  } catch (error) {
    console.error("❌ Error creating offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers/near?lat=27.77&lng=-82.64&radius_km=5
 * Returns all offers to help within a certain radius
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
    const offers = db.collection("offer_help");

    const results = await offers
      .find({
        location: {
          $nearSphere: {
            $geometry: { type: "Point", coordinates: [lng, lat] },
            $maxDistance: radiusKm * 1000, // km → meters
          },
        },
      })
      .limit(100)
      .toArray();

    res.json(results.map((r) => ({ ...r, _id: r._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching nearby offers:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers/:id
 * Fetch a single offer by ObjectId or string id
 */
router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;

    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await offers.findOne(query);
    if (!doc) return res.status(404).json({ error: "Offer not found." });

    res.json({ ...doc, _id: doc._id.toString() });
  } catch (error) {
    console.error("❌ Error fetching offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * DELETE /api/offers/:id
 * Deletes an offer to help by its ID
 */
router.delete("/:id", async (req, res) => {
  try {
    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;

    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const result = await offers.deleteOne(query);

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Offer not found or already deleted." });
    }

    res.json({ message: "Offer deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
