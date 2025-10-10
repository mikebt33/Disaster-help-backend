import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/offers
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
