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
      return res.status(400).json({
        error: "Latitude (lat) and longitude (lng) are required.",
      });
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
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
      followers: [],
      votes: {}, // ✅ store user votes here
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
 * GET /api/help-requests
 */
router.get("/", async (_req, res) => {
  try {
    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const results = await helpRequests.find({}).sort({ timestamp: -1 }).limit(100).toArray();
    res.json(results.map((r) => ({ ...r, _id: r._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching help requests:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/help-requests/near
 */
router.get("/near", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km || 5);

    if (isNaN(lat) || isNaN(lng)) {
      return res.status(400).json({ error: "Valid lat and lng query parameters are required." });
    }

    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const results = await helpRequests
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
    console.error("❌ Error fetching nearby help requests:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/help-requests/:id
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
    if (!doc) return res.status(404).json({ error: "Help request not found." });
    res.json({ ...doc, _id: doc._id.toString() });
  } catch (error) {
    console.error("❌ Error fetching help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/help-requests/:id/confirm
 */
router.patch("/:id/confirm", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await helpRequests.findOne(query);
    if (!doc) return res.status(404).json({ error: "Help request not found." });

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

    await helpRequests.updateOne(query, update);
    res.json({ message: "Confirm recorded." });
  } catch (error) {
    console.error("❌ Error confirming help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/help-requests/:id/dispute
 */
router.patch("/:id/dispute", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await helpRequests.findOne(query);
    if (!doc) return res.status(404).json({ error: "Help request not found." });

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

    await helpRequests.updateOne(query, update);
    res.json({ message: "Dispute recorded." });
  } catch (error) {
    console.error("❌ Error disputing help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/help-requests/:id/resolve
 */
router.patch("/:id/resolve", async (req, res) => {
  try {
    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const result = await helpRequests.updateOne(query, {
      $set: { resolved: true, resolvedAt: new Date() },
    });
    if (result.matchedCount === 0)
      return res.status(404).json({ error: "Help request not found." });
    res.json({ message: "Help request resolved." });
  } catch (error) {
    console.error("❌ Error resolving help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * PATCH /api/help-requests/:id/follow
 */
router.patch("/:id/follow", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: "user_id is required." });

    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await helpRequests.findOne(query);
    if (!doc) return res.status(404).json({ error: "Help request not found." });

    const alreadyFollowing = doc.followers?.includes(user_id);
    const update = alreadyFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await helpRequests.updateOne(query, update);

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
 * POST /api/help-requests/:id/comments
 */
router.post("/:id/comments", async (req, res) => {
  try {
    const { user_id, text } = req.body;
    if (!text) return res.status(400).json({ error: "Comment text is required." });

    const db = getDB();
    const comments = db.collection("help_comments");
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const helpDoc = await helpRequests.findOne(query);
    if (!helpDoc) return res.status(404).json({ error: "Help request not found." });

    const comment = {
      help_request_id: helpDoc._id,
      user_id: user_id || null,
      text,
      createdAt: new Date(),
    };

    const result = await comments.insertOne(comment);
    res.status(201).json({ id: result.insertedId.toString(), ...comment });
  } catch (error) {
    console.error("❌ Error adding comment:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * GET /api/help-requests/:id/comments
 */
router.get("/:id/comments", async (req, res) => {
  try {
    const db = getDB();
    const comments = db.collection("help_comments");
    const { id } = req.params;
    const filter = /^[0-9a-fA-F]{24}$/.test(id)
      ? { help_request_id: new ObjectId(id) }
      : { help_request_id: id };

    const docs = await comments.find(filter).sort({ createdAt: 1 }).toArray();
    res.json(docs.map((c) => ({ ...c, _id: c._id.toString() })));
  } catch (error) {
    console.error("❌ Error fetching comments:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * DELETE /api/help-requests/:id
 */
router.delete("/:id", async (req, res) => {
  try {
    const db = getDB();
    const helpRequests = db.collection("help_requests");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const result = await helpRequests.deleteOne(query);
    if (result.deletedCount === 0)
      return res.status(404).json({ error: "Help request not found or already deleted." });
    res.json({ message: "Help request deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting help request:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
