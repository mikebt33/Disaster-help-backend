import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/offers
 * Create a new offer
 */
router.post("/", async (req, res) => {
  try {
    const { user_id, capabilities, message, lat, lng, available_until } = req.body;
    if (!lat || !lng)
      return res.status(400).json({ error: "Latitude and longitude are required." });

    const db = getDB();
    const offers = db.collection("offer_help");

    const doc = {
      user_id: user_id || null,
      capabilities: capabilities || [],
      message: message || "",
      location: { type: "Point", coordinates: [parseFloat(lng), parseFloat(lat)] },
      available_until: available_until ? new Date(available_until) : null,
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
      followers: [],
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
 * GET /api/offers
 * Returns all offers (most recent first)
 */
router.get("/", async (req, res) => {
  try {
    const db = getDB();
    const offers = db.collection("offer_help");

    const results = await offers
      .find({})
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();

    res.json(results.map((r) => ({ ...r, _id: r._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching offers:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers/near
 * Returns offers within radius (km)
 */
router.get("/near", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km || 5);

    if (isNaN(lat) || isNaN(lng)) {
      return res.status(400).json({ error: "Valid lat and lng required." });
    }

    const db = getDB();
    const offers = db.collection("offer_help");

    const results = await offers
      .find({
        location: {
          $nearSphere: {
            $geometry: { type: "Point", coordinates: [lng, lat] },
            $maxDistance: radiusKm * 1000,
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
 * PATCH confirm/dispute/resolve
 */
["confirm", "dispute", "resolve"].forEach((path) => {
  const updates = {
    confirm: { $inc: { confirmCount: 1 } },
    dispute: { $inc: { disputeCount: 1 } },
    resolve: { $set: { resolved: true, resolvedAt: new Date() } },
  };
  const messages = {
    confirm: "Confirm recorded.",
    dispute: "Dispute recorded.",
    resolve: "Offer resolved.",
  };

  router.patch(`/:id/${path}`, async (req, res) => {
    try {
      const db = getDB();
      const offers = db.collection("offer_help");
      const { id } = req.params;
      const query = /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

      const result = await offers.updateOne(query, updates[path]);
      if (result.matchedCount === 0)
        return res.status(404).json({ error: "Offer not found." });
      res.json({ message: messages[path] });
    } catch (error) {
      console.error(`❌ Error updating offer (${path}):`, error);
      res.status(500).json({ error: "Internal server error." });
    }
  });
});

/**
 * PATCH /api/offers/:id/follow
 */
router.patch("/:id/follow", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await offers.findOne(query);
    if (!doc) return res.status(404).json({ error: "Offer not found." });

    const alreadyFollowing = doc.followers?.includes(user_id);
    const update = alreadyFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await offers.updateOne(query, update);

    res.json({
      message: alreadyFollowing ? "Unfollowed" : "Followed",
      following: !alreadyFollowing,
    });
  } catch (error) {
    console.error("❌ Error toggling follow:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * POST /api/offers/:id/comments
 */
router.post("/:id/comments", async (req, res) => {
  try {
    const { user_id, text } = req.body;
    if (!text) return res.status(400).json({ error: "Comment text is required." });

    const db = getDB();
    const comments = db.collection("offer_comments");
    const offers = db.collection("offer_help");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const offerDoc = await offers.findOne(query);
    if (!offerDoc) return res.status(404).json({ error: "Offer not found." });

    const comment = {
      offer_id: offerDoc._id,
      user_id: user_id || null,
      text,
      createdAt: new Date(),
    };

    const result = await comments.insertOne(comment);
    res.status(201).json({ id: result.insertedId.toString(), ...comment });
  } catch (error) {
    console.error("❌ Error adding offer comment:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers/:id/comments
 */
router.get("/:id/comments", async (req, res) => {
  try {
    const db = getDB();
    const comments = db.collection("offer_comments");
    const { id } = req.params;
    const filter = /^[0-9a-fA-F]{24}$/.test(id)
      ? { offer_id: new ObjectId(id) }
      : { offer_id: id };

    const docs = await comments.find(filter).sort({ createdAt: 1 }).toArray();
    res.json(docs.map((c) => ({ ...c, _id: c._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching offer comments:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * DELETE /api/offers/:id
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
    if (result.deletedCount === 0)
      return res.status(404).json({ error: "Offer not found or already deleted." });
    res.json({ message: "Offer deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
