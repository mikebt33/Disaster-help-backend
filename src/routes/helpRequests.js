import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";
import { notifyFollowersOfUpdate } from "../services/notifications.js";
import { notifyNearbyUsers } from "../services/notifyNearbyUsers.js";

const router = express.Router();

/**
 * POST /api/help-requests
 * Create new help request and trigger geo notifications
 */
router.post("/", async (req, res) => {
  try {
    const {
      user_id,
      type,
      message,
      details: incomingDetails,
      types: incomingTypes,
      lat,
      lng,
      emergency,
      region,
    } = req.body;

    if (!lat || !lng)
      return res.status(400).json({ error: "Latitude and longitude required." });

    const db = getDB();
    const coll = db.collection("help_requests");

    // Normalize type fields
    const types = Array.isArray(incomingTypes)
      ? incomingTypes.filter(Boolean)
      : type
      ? [type]
      : ["general"];

    const details = incomingDetails || message || "";

    const doc = {
      user_id: user_id || null,
      type: types[0] || "general",
      types,
      message: details,
      details,
      location: {
        type: "Point",
        coordinates: [parseFloat(lng), parseFloat(lat)],
      },
      emergency: emergency === true || emergency === "true",
      status: "open",
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
      followers: user_id ? [user_id] : [],
      votes: {},
      timestamp: new Date(),
    };

    const result = await coll.insertOne(doc);
    const inserted = { ...doc, _id: result.insertedId };

    // 📦 Archive successful help request creation (pilot metrics)
    try {
      await db.collection("events_archive").insertOne({
        type: "help_request",
        entityId: result.insertedId,
        user_id: user_id || null,
        emergency: doc.emergency === true,
        region: region || "unknown",
        status: "success",
        timestamp: new Date(),
      });
    } catch (archiveErr) {
      console.error(
        "events_archive (help_request) failed:",
        archiveErr.message
      );
    }

    // ✅ Fire notifications asynchronously, excluding poster’s tokens
    setImmediate(async () => {
      try {
        const poster = doc.user_id
          ? await db.collection("users").findOne({ user_id: doc.user_id })
          : null;

        const excludeTokens = Array.isArray(poster?.fcm_tokens)
          ? poster.fcm_tokens
          : [];

        const insertedWithTokens = { ...inserted, fcm_tokens: excludeTokens };

        await notifyNearbyUsers("help_requests", insertedWithTokens, {
          excludeUserId: doc.user_id,
          excludeTokens,
        });
      } catch (err) {
        console.error("notifyNearbyUsers (help_requests) error:", err);
      }
    });

    res.status(201).json({ id: result.insertedId.toString(), ...doc });
  } catch (err) {
    console.error("POST /api/help-requests error:", err);

    // ❌ Archive failed help request attempt
    try {
      const db = getDB();
      await db.collection("events_archive").insertOne({
        type: "help_request",
        status: "error",
        errorMessage: err.message,
        timestamp: new Date(),
      });
    } catch (archiveErr) {
      console.error(
        "events_archive (help_request error) failed:",
        archiveErr.message
      );
    }

    res.status(500).json({ error: "Internal server error." });
  }
});

/** GET all / near / by id **/
router.get("/", async (_req, res) => {
  try {
    const db = getDB();
    const docs = await db
      .collection("help_requests")
      .find({})
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();

    res.json(docs.map((d) => ({ ...d, _id: d._id.toString() })));
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.get("/near", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km || 5);

    if (isNaN(lat) || isNaN(lng))
      return res.status(400).json({ error: "Valid lat/lng required." });

    const db = getDB();
    const results = await db
      .collection("help_requests")
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
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.get("/:id", async (req, res) => {
  try {
    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const doc = await db.collection("help_requests").findOne(query);
    if (!doc) return res.status(404).json({ error: "Help request not found." });

    res.json({ ...doc, _id: doc._id.toString() });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

/** confirm / dispute / resolve **/
router.patch("/:id/confirm", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id)
      return res.status(400).json({ error: "user_id required." });

    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const coll = db.collection("help_requests");
    const doc = await coll.findOne(query);
    if (!doc) return res.status(404).json({ error: "Not found." });

    const votes = doc.votes || {};
    const cur = votes[user_id];
    if (cur === "confirm") return res.json({ message: "Already confirmed." });

    const update = { $set: { [`votes.${user_id}`]: "confirm" }, $inc: {} };
    if (cur === "dispute") {
      update.$inc.confirmCount = 1;
      update.$inc.disputeCount = -1;
    } else update.$inc.confirmCount = 1;

    await coll.updateOne(query, update);

    setImmediate(() =>
      notifyFollowersOfUpdate(
        "help_requests",
        id,
        user_id,
        "confirm",
        "A help request was confirmed."
      )
    );

    res.json({ message: "Confirm recorded." });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.patch("/:id/dispute", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id)
      return res.status(400).json({ error: "user_id required." });

    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const coll = db.collection("help_requests");
    const doc = await coll.findOne(query);
    if (!doc) return res.status(404).json({ error: "Not found." });

    const votes = doc.votes || {};
    const cur = votes[user_id];
    if (cur === "dispute") return res.json({ message: "Already disputed." });

    const update = { $set: { [`votes.${user_id}`]: "dispute" }, $inc: {} };
    if (cur === "confirm") {
      update.$inc.confirmCount = -1;
      update.$inc.disputeCount = 1;
    } else update.$inc.disputeCount = 1;

    await coll.updateOne(query, update);

    setImmediate(() =>
      notifyFollowersOfUpdate(
        "help_requests",
        id,
        user_id,
        "dispute",
        "A help request was disputed."
      )
    );

    res.json({ message: "Dispute recorded." });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.patch("/:id/resolve", async (req, res) => {
  try {
    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const coll = db.collection("help_requests");
    const r = await coll.updateOne(query, {
      $set: { resolved: true, resolvedAt: new Date() },
    });

    if (!r.matchedCount)
      return res.status(404).json({ error: "Not found." });

    setImmediate(() =>
      notifyFollowersOfUpdate(
        "help_requests",
        id,
        null,
        "resolve",
        "A followed help request has been resolved."
      )
    );

    res.json({ message: "Resolved." });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

/** follow **/
router.patch("/:id/follow", async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id)
      return res.status(400).json({ error: "user_id required." });

    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const coll = db.collection("help_requests");
    const doc = await coll.findOne(query);
    if (!doc) return res.status(404).json({ error: "Not found." });

    const followers = doc.followers || [];
    const alreadyFollowing = followers.includes(user_id);
    const update = alreadyFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await coll.updateOne(query, update);

    if (!alreadyFollowing)
      setImmediate(() =>
        notifyFollowersOfUpdate(
          "help_requests",
          id,
          user_id,
          "follow",
          "A post you follow has a new follower."
        )
      );

    res.json({
      message: alreadyFollowing ? "Unfollowed" : "Followed",
      following: !alreadyFollowing,
    });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

/** comments **/
router.post("/:id/comments", async (req, res) => {
  try {
    const { user_id, text, region } = req.body;
    if (!text)
      return res.status(400).json({ error: "Comment text required." });

    const db = getDB();
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const coll = db.collection("help_requests");
    const comments = db.collection("help_comments");

    const helpDoc = await coll.findOne(query);
    if (!helpDoc)
      return res.status(404).json({ error: "Help request not found." });

    const comment = {
      help_request_id: helpDoc._id,
      user_id: user_id || null,
      text,
      createdAt: new Date(),
    };

    const result = await comments.insertOne(comment);

    // 💬 Archive comment
    try {
      await db.collection("comments_archive").insertOne({
        parentType: "help_request",
        parentId: helpDoc._id,
        user_id: user_id || null,
        region: region || "unknown",
        text,
        timestamp: new Date(),
      });
    } catch (archiveErr) {
      console.error(
        "comments_archive (help_request) failed:",
        archiveErr.message
      );
    }

    setImmediate(() =>
      notifyFollowersOfUpdate("help_requests", id, user_id, "comment", text)
    );

    res.status(201).json({ id: result.insertedId.toString(), ...comment });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.get("/:id/comments", async (req, res) => {
  try {
    const db = getDB();
    const id = req.params.id;
    const filter = /^[0-9a-fA-F]{24}$/.test(id)
      ? { help_request_id: new ObjectId(id) }
      : { help_request_id: id };

    const docs = await db
      .collection("help_comments")
      .find(filter)
      .sort({ createdAt: 1 })
      .toArray();

    res.json(docs.map((c) => ({ ...c, _id: c._id.toString() })));
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

router.delete("/:id", async (req, res) => {
  try {
    const db = getDB();
    const id = req.params.id;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };

    const r = await db.collection("help_requests").deleteOne(query);
    if (!r.deletedCount)
      return res.status(404).json({ error: "Not found." });

    res.json({ message: "Help request deleted." });
  } catch {
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
