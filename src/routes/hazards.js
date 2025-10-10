import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/hazards
 * Create a new hazard
 */
router.post("/", async (req, res) => {
  try {
    const { type, description, geometry, severity, source, user_id } = req.body;
    if (["NWS", "FEMA", "USGS"].includes(source)) {
      return res.status(400).json({ error: "Reserved source identifier." });
    }
    if (!geometry || !geometry.type || !geometry.coordinates) {
      return res.status(400).json({ error: "Valid geometry is required." });
    }

    const db = getDB();
    const hazards = db.collection("hazards");
    const doc = {
      user_id: user_id || null,
      type: type || "hazard",
      description: description || "",
      severity: severity || "Unknown",
      source: source || "user",
      geometry,
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
      followers: [],
      votes: {}, // ✅ per-user vote tracking
      timestamp: new Date(),
    };

    const result = await hazards.insertOne(doc);
    res.status(201).json({ id: result.insertedId.toString(), ...doc });
  } catch (error) {
    console.error("❌ Error creating hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards
 */
router.get("/", async (_req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const all = await hazards.find({}).sort({ timestamp: -1 }).limit(100).toArray();
    res.json(all.map((h) => ({ ...h, _id: h._id.toString() })));
  } catch (error) {
    console.error("❌ Error listing hazards:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards/near
 */
router.get("/near", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km || 5);
    if (isNaN(lat) || isNaN(lng))
      return res.status(400).json({ error: "Valid lat and lng required." });

    const db = getDB();
    const hazards = db.collection("hazards");
    const results = await hazards
      .find({
        geometry: {
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
    console.error("❌ Error fetching nearby hazards:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards/:id
 */
router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await hazards.findOne(query);
    if (!doc) return res.status(404).json({ error: "Hazard not found." });
    res.json({ ...doc, _id: doc._id.toString() });
  } catch (error) {
    console.error("❌ Error fetching hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/hazards/:id/confirm
 */
router.patch("/:id/confirm", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };

    const doc = await hazards.findOne(query);
    if (!doc) return res.status(404).json({ error: "Hazard not found." });

    const votes = doc.votes || {};
    const currentVote = votes[user_id];

    if (currentVote === "confirm") {
      return res.status(200).json({ message: "User already confirmed." });
    }

    const update = {};
    if (currentVote === "dispute") {
      update.$inc = { confirmCount: 1, disputeCount: -1 };
    } else {
      update.$inc = { confirmCount: 1 };
    }
    update.$set = { [`votes.${user_id}`]: "confirm" };

    await hazards.updateOne(query, update);
    res.json({ message: "Confirm recorded." });
  } catch (error) {
    console.error("❌ Error confirming hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/hazards/:id/dispute
 */
router.patch("/:id/dispute", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };

    const doc = await hazards.findOne(query);
    if (!doc) return res.status(404).json({ error: "Hazard not found." });

    const votes = doc.votes || {};
    const currentVote = votes[user_id];

    if (currentVote === "dispute") {
      return res.status(200).json({ message: "User already disputed." });
    }

    const update = {};
    if (currentVote === "confirm") {
      update.$inc = { confirmCount: -1, disputeCount: 1 };
    } else {
      update.$inc = { disputeCount: 1 };
    }
    update.$set = { [`votes.${user_id}`]: "dispute" };

    await hazards.updateOne(query, update);
    res.json({ message: "Dispute recorded." });
  } catch (error) {
    console.error("❌ Error disputing hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/hazards/:id/resolve
 */
router.patch("/:id/resolve", async (req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };

    const result = await hazards.updateOne(query, {
      $set: { resolved: true, resolvedAt: new Date() },
    });
    if (result.matchedCount === 0)
      return res.status(404).json({ error: "Hazard not found." });
    res.json({ message: "Hazard resolved." });
  } catch (error) {
    console.error("❌ Error resolving hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/hazards/:id/follow
 */
router.patch("/:id/follow", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };

    const doc = await hazards.findOne(query);
    if (!doc) return res.status(404).json({ error: "Hazard not found." });

    const alreadyFollowing = (doc.followers || []).includes(user_id);
    const update = alreadyFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await hazards.updateOne(query, update);
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
 * POST /api/hazards/:id/comments
 */
router.post("/:id/comments", async (req, res) => {
  try {
    const { user_id, text } = req.body;
    if (!text) return res.status(400).json({ error: "Comment text is required." });

    const db = getDB();
    const comments = db.collection("hazard_comments");
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };

    const hazardDoc = await hazards.findOne(query);
    if (!hazardDoc) return res.status(404).json({ error: "Hazard not found." });

    const comment = {
      hazard_id: hazardDoc._id,
      user_id: user_id || null,
      text,
      createdAt: new Date(),
    };

    const result = await comments.insertOne(comment);
    res.status(201).json({ id: result.insertedId.toString(), ...comment });
  } catch (error) {
    console.error("❌ Error adding hazard comment:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/hazards/:id/comments
 */
router.get("/:id/comments", async (req, res) => {
  try {
    const db = getDB();
    const comments = db.collection("hazard_comments");
    const { id } = req.params;
    const filter =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { hazard_id: new ObjectId(id) }
        : { hazard_id: id };

    const docs = await comments.find(filter).sort({ createdAt: 1 }).toArray();
    res.json(docs.map((c) => ({ ...c, _id: c._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching hazard comments:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * DELETE /api/hazards/:id
 */
router.delete("/:id", async (req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id) ? { _id: new ObjectId(id) } : { _id: id };
    const result = await hazards.deleteOne(query);
    if (result.deletedCount === 0)
      return res
        .status(404)
        .json({ error: "Hazard not found or already deleted." });
    res.json({ message: "Hazard deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
