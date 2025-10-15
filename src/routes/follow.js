import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * PATCH /api/:collection/:id/follow
 */
router.patch("/:collection/:id/follow", async (req, res) => {
  try {
    const { collection, id } = req.params;
    const { user_id } = req.body || {};
    if (!user_id) return res.status(400).json({ error: "user_id is required" });

    const db = getDB();
    const coll = db.collection(collection);

    // Convert string id to ObjectId
    const _id = ObjectId.isValid(id) ? new ObjectId(id) : id;

    const doc = await coll.findOne({ _id });
    if (!doc) return res.status(404).json({ error: "Document not found" });

    const already = (doc.followers || []).includes(user_id);
    const update = already
      ? { $pull: { followers: user_id } }
      : { $addToSet: { followers: user_id } };

    await coll.updateOne({ _id }, update);

    res.json({
      following: !already,
      message: already
        ? "Unfollowed successfully"
        : "Now following this report",
    });
  } catch (e) {
    console.error("❌ follow route failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
