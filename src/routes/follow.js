// src/routes/follow.js
import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

// ✅ Health check to confirm route loaded
router.get("/follow-test", (_req, res) => {
  res.json({ ok: true, msg: "✅ follow route active" });
});

// Map friendly names → actual MongoDB collections
function normalizeCollection(c) {
  if (c === "offers") return "offer_help";
  if (c === "help-requests") return "help_requests";
  if (c === "hazards") return "hazards";
  return c;
}

/**
 * POST or PATCH /api/:collection/:id/follow
 * Toggles follow/unfollow for a given user_id.
 */
router.all("/:collection/:id/follow", async (req, res) => {
  try {
    const { collection, id } = req.params;
    const { user_id } = req.body || {};

    console.log(`[Follow] hit: ${collection}/${id} from ${user_id}`);

    if (!user_id) {
      return res.status(400).json({ error: "user_id is required" });
    }

    const db = getDB();
    const collName = normalizeCollection(collection);
    const coll = db.collection(collName);

    const _id = ObjectId.isValid(id) ? new ObjectId(id) : id;
    const doc = await coll.findOne({ _id });

    if (!doc) {
      console.warn(`[Follow] ❌ Document not found in ${collName} for id=${id}`);
      return res.status(404).json({ error: "Document not found" });
    }

    const followers = doc.followers || [];
    const isFollowing = followers.includes(user_id);

    const update = isFollowing
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await coll.updateOne({ _id }, update);

    const msg = isFollowing
      ? "Unfollowed successfully"
      : "Now following this report";

    console.log(`[Follow] ✅ ${msg} (${collName}/${id})`);
    res.json({ following: !isFollowing, message: msg });
  } catch (e) {
    console.error("❌ follow route failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
