import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";
import { notifyFollowersOfUpdate } from "../services/notifications.js";

const router = express.Router();

/**
 * ✅ Quick sanity check endpoint
 */
router.get("/follow-test", (_req, res) => {
  res.json({ ok: true, msg: "✅ follow route active" });
});

/**
 * Normalize collection names for MongoDB
 */
function normalizeCollection(c) {
  switch (c) {
    case "offers":
      return "offer_help";
    case "help-requests":
      return "help_requests";
    case "hazards":
      return "hazards";
    default:
      return c;
  }
}

/**
 * PATCH /api/:collection/:id/follow
 * Toggles follow/unfollow for any post type.
 */
router.all("/:collection/:id/follow", async (req, res) => {
  try {
    const { collection, id } = req.params;
    const { user_id } = req.body || {};
    if (!user_id) return res.status(400).json({ error: "user_id is required" });

    const db = getDB();
    const collName = normalizeCollection(collection);
    const coll = db.collection(collName);

    const _id = ObjectId.isValid(id) ? new ObjectId(id) : id;
    const doc = await coll.findOne({ _id });
    if (!doc) return res.status(404).json({ error: "Document not found" });

    const followers = doc.followers || [];
    const isFollowing = followers.includes(user_id);
    const update = isFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await coll.updateOne({ _id }, update);

    // Trigger notification only when a user starts following
    if (!isFollowing) {
      setImmediate(() =>
        notifyFollowersOfUpdate(
          collName,
          id,
          user_id,
          "follow",
          "A post you follow has a new follower."
        )
      );
    }

    res.json({
      message: isFollowing ? "Unfollowed successfully" : "Now following this report",
      following: !isFollowing,
    });
  } catch (e) {
    console.error("❌ follow route failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
