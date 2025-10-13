import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";
import { notifyFollowersOfUpdate } from "../services/notifications.js";
import { notifyNearbyUsers } from "../services/notifyNearbyUsers.js";

const router = express.Router();

/**
 * POST /api/offers
 * Create a new offer
 * ✅ Adds `type` for frontend compatibility
 * ✅ Triggers geo-based notifications for nearby users
 * ✅ Skips creator + dedupe guard
 */
router.post("/", async (req, res) => {
  try {
    const { user_id, capabilities, type, message, lat, lng, available_until } = req.body;

    if (!lat || !lng) {
      return res.status(400).json({ error: "Latitude and longitude are required." });
    }

    const db = getDB();
    const offers = db.collection("offer_help");

    const typeValue = Array.isArray(capabilities)
      ? capabilities.join(", ")
      : type || "Unspecified";

    const doc = {
      user_id: user_id || null,
      type: typeValue,
      capabilities: Array.isArray(capabilities)
        ? capabilities
        : typeValue.split(", "),
      message: message || "",
      location: {
        type: "Point",
        coordinates: [parseFloat(lng), parseFloat(lat)],
      },
      available_until: available_until ? new Date(available_until) : null,
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
      followers: user_id ? [user_id] : [],
      votes: {},
      timestamp: new Date(),
    };

    const result = await offers.insertOne(doc);
    const inserted = { ...doc, _id: result.insertedId };

    // ✅ Geo push (dedupe-safe, skip creator)
    setImmediate(async () => {
      try {
        await notifyNearbyUsers("offer_help", inserted, { excludeUserId: doc.user_id });
        console.log(`📡 notifyNearbyUsers fired for offer ${result.insertedId}`);
      } catch (err) {
        console.error("❌ notifyNearbyUsers failed:", err);
      }
    });

    res.status(201).json({ id: result.insertedId.toString(), ...doc });
  } catch (error) {
    console.error("❌ Error creating offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers
 */
router.get("/", async (_req, res) => {
  try {
    const db = getDB();
    const offers = db.collection("offer_help");
    const results = await offers.find({}).sort({ timestamp: -1 }).limit(100).toArray();
    res.json(results.map((r) => ({ ...r, _id: r._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching offers:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/offers/near
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
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
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
 * PATCH /api/offers/:id/confirm
 */
router.patch("/:id/confirm", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

    const doc = await offers.findOne(query);
    if (!doc) return res.status(404).json({ error: "Offer not found." });

    const votes = doc.votes || {};
    const currentVote = votes[user_id];

    if (currentVote === "confirm") {
      return res.status(200).json({ message: "User already confirmed." });
    }

    const update = { $set: { [`votes.${user_id}`]: "confirm" }, $inc: {} };
    if (currentVote === "dispute") {
      update.$inc.confirmCount = 1;
      update.$inc.disputeCount = -1;
    } else {
      update.$inc.confirmCount = 1;
    }

    await offers.updateOne(query, update);

    setImmediate(() =>
      notifyFollowersOfUpdate("offer_help", id, user_id, "confirm", "An offer was confirmed.")
    );

    res.json({ message: "Confirm recorded." });
  } catch (error) {
    console.error("❌ Error confirming offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/offers/:id/dispute
 */
router.patch("/:id/dispute", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

    const doc = await offers.findOne(query);
    if (!doc) return res.status(404).json({ error: "Offer not found." });

    const votes = doc.votes || {};
    const currentVote = votes[user_id];

    if (currentVote === "dispute") {
      return res.status(200).json({ message: "User already disputed." });
    }

    const update = { $set: { [`votes.${user_id}`]: "dispute" }, $inc: {} };
    if (currentVote === "confirm") {
      update.$inc.confirmCount = -1;
      update.$inc.disputeCount = 1;
    } else {
      update.$inc.disputeCount = 1;
    }

    await offers.updateOne(query, update);

    setImmediate(() =>
      notifyFollowersOfUpdate("offer_help", id, user_id, "dispute", "An offer was disputed.")
    );

    res.json({ message: "Dispute recorded." });
  } catch (error) {
    console.error("❌ Error disputing offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/offers/:id/resolve
 */
router.patch("/:id/resolve", async (req, res) => {
  try {
    const db = getDB();
    const offers = db.collection("offer_help");
    const { id } = req.params;
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

    const result = await offers.updateOne(query, {
      $set: { resolved: true, resolvedAt: new Date() },
    });
    if (result.matchedCount === 0)
      return res.status(404).json({ error: "Offer not found." });

    setImmediate(() =>
      notifyFollowersOfUpdate("offer_help", id, null, "resolve", "A followed offer has been marked as resolved.")
    );

    res.json({ message: "Offer resolved." });
  } catch (error) {
    console.error("❌ Error resolving offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
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
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

    const doc = await offers.findOne(query);
    if (!doc) return res.status(404).json({ error: "Offer not found." });

    const followers = doc.followers || [];
    const alreadyFollowing = followers.includes(user_id);

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
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
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

    setImmediate(() =>
      notifyFollowersOfUpdate("offer_help", id, user_id, "comment", text)
    );

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
    const filter =
      /^[0-9a-fA-F]{24}$/.test(id)
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
    const query =
      /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

    const result = await offers.deleteOne(query);
    if (result.deletedCount === 0)
      return res
        .status(404)
        .json({ error: "Offer not found or already deleted." });
    res.json({ message: "Offer deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting offer:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
