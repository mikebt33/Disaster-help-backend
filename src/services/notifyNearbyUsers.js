/**
 * /src/services/notifyNearbyUsers.js
 * -------------------------------------------------------------
 * Centralized geofence-based notification logic.
 * Triggers when a new hazard/help/offer post is created.
 *
 * Improvements:
 *   ✅ Deduplicate FCM tokens across all matched users
 *   ✅ Skip notifying the creator (optional)
 *   ✅ Event-level send guard (TTL) to avoid double-sends
 *   ✅ Android collapseKey + notification.tag to coalesce duplicates
 *   ✅ iOS apns-collapse-id to coalesce duplicates
 *   ✅ Clean invalid tokens automatically
 *   ✅ Robust logging + safe defaults for radius
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js";

// --- In-memory send guard (use Redis if you run >1 instance) ---
const recentlySent = new Map();            // key -> timestamp
const TTL_MS = 60_000;                     // 60s dedupe window
const DEFAULT_RADIUS_MI = Number(process.env.DEFAULT_RADIUS_MI || 10);

function _markSent(key) {
  recentlySent.set(key, Date.now());
  setTimeout(() => {
    const ts = recentlySent.get(key);
    if (ts && Date.now() - ts > TTL_MS) recentlySent.delete(key);
  }, TTL_MS + 5_000);
}

/**
 * Notify all users within their configured radius of a new event.
 *
 * @param {string} collection - "hazards" | "help_requests" | "offer_help"
 * @param {object} doc - The newly created document; must contain _id and geometry/location
 * @param {object} [opts]
 * @param {string} [opts.excludeUserId] - user_id to exclude (usually the creator)
 */
export async function notifyNearbyUsers(collection, doc, opts = {}) {
  const sendKey = `geo:${collection}:${doc?._id}`;
  try {
    // --- Dedupe guard ---
    if (!doc?._id) {
      console.warn(`[PUSH][geo] ⚠️ Missing doc._id for ${collection}, abort.`);
      return;
    }
    const last = recentlySent.get(sendKey);
    if (last && Date.now() - last < TTL_MS) {
      console.log(`[PUSH][geo] ⏩ Skipping duplicate send for ${sendKey}`);
      return;
    }

    // --- Extract event lat/lng ---
    let eventLat = 0, eventLng = 0;
    if (doc?.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.geometry.coordinates;
    } else if (doc?.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.location.coordinates;
    } else if (typeof doc.lat === "number" && typeof doc.lng === "number") {
      eventLat = doc.lat; eventLng = doc.lng;
    } else {
      console.warn("[PUSH][geo] ⚠️ Event missing geometry/location; abort.");
      return;
    }

    console.log(
      `[PUSH][geo] ▶ ${collection}/${doc._id} at lat=${eventLat}, lng=${eventLng}`
    );

    // --- Load candidate users ---
    const db = getDB();
    const users = db.collection("users");

    // ⚠️ Important change: don't require radiusMi in the query.
    // Some users may not have set it yet; we'll fallback to DEFAULT_RADIUS_MI.
    const candidates = await users
      .find({
        lastLocation: { $exists: true },
        fcm_tokens: { $exists: true, $ne: [] },
      })
      .project({ user_id: 1, fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    console.log(`[PUSH][geo] candidates found: ${candidates.length}`);

    if (!candidates.length) {
      console.log("[PUSH][geo] ℹ️ No users with lastLocation + fcm_tokens.");
      return;
    }

  // --- Build unique token set within geofence ---
  const excludeUserId = opts.excludeUserId ?? doc.user_id;
  const excludeTokens = new Set(
    (opts.excludeTokens ?? []).filter(t => typeof t === "string" && t.length > 10)
  );

  const tokenSet = new Set();
  let considered = 0, inside = 0, skippedNoLoc = 0, skippedCreator = 0, skippedDevice = 0;

  for (const u of candidates) {
    if (!u?.lastLocation?.lat || !u?.lastLocation?.lng) {
      skippedNoLoc++;
      continue;
    }
    considered++;

    // ✅ Skip the creator
    if (
      excludeUserId &&
      u.user_id &&
      u.user_id.toString().trim() === excludeUserId.toString().trim()
    ) {
      skippedCreator++;
      continue;
    }

    // ✅ Skip same physical device (duplicate tokens)
    if (
      Array.isArray(doc.fcm_tokens) &&
      doc.fcm_tokens.length > 0 &&
      Array.isArray(u.fcm_tokens) &&
      u.fcm_tokens.some(t => doc.fcm_tokens.includes(t))
    ) {
      skippedDevice++;
      continue;
    }

    // ✅ Geofence filter (Haversine distance)
    const userRadiusMi = u.radiusMi || DEFAULT_RADIUS_MI;
    const distMi = haversineDistanceMi(
      eventLat,
      eventLng,
      u.lastLocation.lat,
      u.lastLocation.lng
    );
    if (distMi > userRadiusMi) continue; // outside user’s radius

    inside++;

    // ✅ Add valid FCM tokens
    for (const t of u.fcm_tokens || []) {
      if (typeof t === "string" && t.length > 10 && !excludeTokens.has(t)) {
        tokenSet.add(t);
      }
    }
  }

  const tokens = Array.from(tokenSet);
  console.log(
    `[PUSH][geo] considered=${considered}, inside=${inside}, skippedCreator=${skippedCreator}, skippedDevice=${skippedDevice}, noLoc=${skippedNoLoc}, uniqueTokens=${tokens.length}`
  );

    // --- Compose notification ---
    const titleMap = {
      hazards: "⚠️ New Hazard Reported Nearby",
      help_requests: "🚨 Help Request Near You",
      offer_help: "💚 Offer to Help in Your Area",
    };

    const title = titleMap[collection] || "📍 New Update in Your Area";
    const body =
      doc.description ||
      doc.message ||
      doc.type ||
      "A new report has been posted near your location.";

    const collapseKey = `geo_${collection}_${doc._id}`;

    const message = {
      data: {
        title,
        body,
        deeplink: `disasterhelp://detail?c=${collection}&id=${doc._id}`,
        collection,
        docId: String(doc._id),
      },
      tokens,
      android: {
        priority: "high",
        collapseKey,
        notification: {
          channelId: "alerts",
          tag: collapseKey,
        },
      },
      apns: {
        headers: {
          "apns-priority": "10",
          "apns-collapse-id": collapseKey,
        },
        payload: {
          aps: { sound: "default" },
        },
      },
    };

    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `[PUSH][geo] 📤 Sent to ${tokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    // --- Clean invalid tokens ---
    const invalid = [];
    response.responses.forEach((r, i) => {
      if (!r.success) {
        const code = r.error?.code || "unknown";
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          invalid.push(tokens[i]);
        } else {
          console.warn("[PUSH][geo] send error:", code);
        }
      }
    });

    if (invalid.length > 0) {
      await users.updateMany(
        { fcm_tokens: { $in: invalid } },
        { $pull: { fcm_tokens: { $in: invalid } } }
      );
      console.log(`[PUSH][geo] 🧹 Removed ${invalid.length} invalid tokens.`);
    }

    _markSent(sendKey);
  } catch (err) {
    console.error(`[PUSH][geo] ❌ Error for ${sendKey}:`, err);
  }
}
