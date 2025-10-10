import express from "express";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

const router = express.Router();

/**
 * POST /api/hazards
 */
router.post("/", async (req, res) => {
  try {
    const { type, description, geometry, severity, source } = req.body;
    if (["NWS", "FEMA", "USGS"].includes(source)) {
      return res.status(400).json({ error: "Reserved source identifier." });
    }
    if (!geometry || !geometry.type || !geometry.coordinates) {
      return res.status(400).json({ error: "Valid geometry is required." });
    }

    const db = getDB();
    const hazards = db.collection("hazards");
    const doc = {
      type: type || "hazard",
      description: description || "",
      severity: severity || "Unknown",
      source: source || "user",
      geometry,
      confirmCount: 0,
      disputeCount: 0,
      resolved: false,
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
 * PATCH confirm/dispute/resolve
 */
const patchOps = [
  { path: "confirm", update: { $inc: { confirmCount: 1 } }, msg: "Confirm recorded." },
  { path: "dispute", update: { $inc: { disputeCount: 1 } }, msg: "Dispute recorded." },
  { path: "resolve", update: { $set: { resolved: true, resolvedAt: new Date() } }, msg: "Hazard resolved." },
];

patchOps.forEach(({ path, update, msg }) => {
  router.patch(`/:id/${path}`, async (req, res) => {
    try {
      const db = getDB();
      const hazards = db.collection("hazards");
      const { id } = req.params;
      const query = /^[0-9a-fA-F]{24}$/.test(id)
        ? { _id: new ObjectId(id) }
        : { _id: id };

      const result = await hazards.updateOne(query, update);
      if (result.matchedCount === 0)
        return res.status(404).json({ error: "Hazard not found." });
      res.json({ message: msg });
    } catch (error) {
      console.error(`❌ Error updating hazard (${path}):`, error);
      res.status(500).json({ error: "Internal server error." });
    }
  });
});

/**
 * DELETE /api/hazards/:id
 */
router.delete("/:id", async (req, res) => {
  try {
    const db = getDB();
    const hazards = db.collection("hazards");
    const { id } = req.params;
    const query = /^[0-9a-fA-F]{24}$/.test(id)
      ? { _id: new ObjectId(id) }
      : { _id: id };
    const result = await hazards.deleteOne(query);
    if (result.deletedCount === 0)
      return res.status(404).json({ error: "Hazard not found or already deleted." });
    res.json({ message: "Hazard deleted successfully." });
  } catch (error) {
    console.error("❌ Error deleting hazard:", error);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
