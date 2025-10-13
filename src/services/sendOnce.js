// /src/services/sendOnce.js
import { getDB } from "../db.js";

/**
 * Allow exactly one send per (key) within ttlMs.
 * Works across multiple server instances (uses Mongo as the guard).
 * - Creates/updates a doc in 'sent_events' with lastSentAt.
 * - If another process has already written within the TTL, we skip.
 */
export async function shouldSendNow(key, ttlMs = 60_000) {
  const db = getDB();
  const coll = db.collection("sent_events");
  const now = new Date();
  const threshold = new Date(now.getTime() - ttlMs);

  try {
    // Try an atomic upsert that only succeeds if the doc is stale or missing.
    // If a fresh doc exists (within TTL), the upsert will try to insert and
    // will hit E11000 (duplicate key) — we treat that as "do not send".
    const res = await coll.findOneAndUpdate(
      {
        _id: key,
        $or: [{ lastSentAt: { $lt: threshold } }, { lastSentAt: { $exists: false } }],
      },
      { $set: { lastSentAt: now }, $inc: { count: 1 } },
      { upsert: true, returnDocument: "after" }
    );

    // If we got a value back, we either inserted or updated a stale doc → OK to send.
    return !!res.value;
  } catch (err) {
    // Upsert attempted but doc already exists with a fresh lastSentAt -> duplicate key error.
    if (err && err.code === 11000) {
      return false;
    }
    console.error("sendOnce/shouldSendNow error:", err);
    // Fail-open: if guard errors, don't block notifications entirely.
    return true;
  }
}
