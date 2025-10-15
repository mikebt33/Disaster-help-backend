// src/routes/follow.js
import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

// Simple health check to confirm this route is active
router.get("/follow-test", (_req, res) => {
  res.json({ ok: true, msg: "✅ follow route active" });
});

/**
 * POST or PATCH /api/:collection/:id/follow
 * Toggles follow/unfollow for a given user_id.
 */
router.all("/:collection/:id/follow", async (req, res) => {
  try {
    const { collection, id } = req.params;
    const { user_id } = req.body || {};

    if (!user_id) {
      return res.status(400).json({ error: "user_id is required" });
    }

    const db = getDB();
    if (!db.collection(collection)) {
      return res.status(400).json({ error: `Invalid collection: ${collection}` });
    }

    const coll = db.collection(collection);
    const _id = ObjectId.isValid(id) ? new ObjectId(id) : id;
    const doc = await coll.findOne({ _id });

    if (!doc) {
      return res.status(404).json({ error: "Document not found" });
    }

    const followers = doc.followers || [];
    const isFollowing = followers.includes(user_id);

    const update = isFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await coll.updateOne({ _id }, update);

    res.json({
      following: !isFollowing,
      message: isFollowing
        ? "Unfollowed successfully"
        : "Now following this report",
    });
  } catch (e) {
    console.error("❌ follow route failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
