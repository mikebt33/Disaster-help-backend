/**
 * /src/services/notifyNearbyUsers.js
 * -------------------------------------------------------------
 * Centralized geofence-based notification logic.
 * Triggers when a new hazard/help/offer post is created.
 *
 * Improvements in this version:
 *   ✅ Deduplicate FCM tokens across all matched users
 *   ✅ Skip notifying the creator (excludeUserId)
 *   ✅ In-process event-level send guard (TTL)
 *   ✅ Android collapseKey + notification.tag coalesce duplicates
 *   ✅ iOS apns-collapse-id coalesces duplicates
 *   ✅ Clean invalid tokens automatically
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js";

// 🧠 Local memory TTL (simple, works even without Redis)
const recentlySent = new Map(); // key -> timestamp
const TTL_MS = 60_000; // 60 s dedupe window

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
 * @param {string} collection  - "hazards" | "help_requests" | "offer_help"
 * @param {object} doc         - The newly created document
 * @param {object} [opts]
 * @param {string} [opts.excludeUserId] - User to exclude (usually the creator)
 */
export async function notifyNearbyUsers(collection, doc, opts = {}) {
  try {
    const db = getDB();
    const users = db.collection("users");

    // 🧩 Global event dedupe key
    const sendKey = `geo:${collection}:${doc._id}`;
    const last = recentlySent.get(sendKey);
    if (last && Date.now() - last < TTL_MS) {
      console.log(`⏩ Skipping duplicate geo-send for ${sendKey}`);
      return;
    }

    // 🔍 Extract event location
    let eventLat = 0,
      eventLng = 0;
    if (doc?.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.geometry.coordinates;
    } else if (doc?.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.location.coordinates;
    } else if (typeof doc.lat === "number" && typeof doc.lng === "number") {
      eventLat = doc.lat;
      eventLng = doc.lng;
    } else {
      console.warn("⚠️ notifyNearbyUsers: event missing geometry/location");
      return;
    }

    // 🧭 Fetch all users with location + radius + FCM token
    const candidates = await users
      .find({
        lastLocation: { $exists: true },
        radiusMi: { $gt: 0 },
        fcm_tokens: { $exists: true, $ne: [] },
      })
      .project({ user_id: 1, fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    if (!candidates.length) {
      console.log("ℹ️ No users with location or tokens found.");
      return;
    }

    // ✅ Build deduped token set
    const excludeUserId = opts.excludeUserId ?? doc.user_id;
    const tokenSet = new Set();

    for (const u of candidates) {
      if (!u.lastLocation || !u.radiusMi) continue;
      if (excludeUserId && u.user_id === excludeUserId) continue;

      const dist = haversineDistanceMi(
        u.lastLocation.lat,
        u.lastLocation.lng,
        eventLat,
        eventLng
      );
      if (!isNaN(dist) && dist <= u.radiusMi) {
        for (const t of u.fcm_tokens || []) {
          if (typeof t === "string" && t.length > 10) tokenSet.add(t);
        }
      }
    }

    const tokens = Array.from(tokenSet);
    if (tokens.length === 0) {
      console.log(`ℹ️ No nearby users to notify for ${collection}.`);
      return;
    }

    // 📰 Compose message
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

    // ✅ collapseKey ensures duplicate deliveries (system + local) merge
    const collapseKey = `geo_${collection}_${doc._id}`;

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${doc._id}`,
        collection,
        docId: String(doc._id),
      },
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
      tokens,
    };

    // 📨 Send FCM
    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `📤 Geo-notif to ${tokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    // 🧹 Clean invalid tokens
    const invalidTokens = [];
    response.responses.forEach((r, i) => {
      if (!r.success) {
        const code = r.error?.code || "unknown";
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          invalidTokens.push(tokens[i]);
        }
      }
    });

    if (invalidTokens.length > 0) {
      await users.updateMany(
        { fcm_tokens: { $in: invalidTokens } },
        { $pull: { fcm_tokens: { $in: invalidTokens } } }
      );
      console.log(`🧹 Removed ${invalidTokens.length} invalid tokens.`);
    }

    _markSent(sendKey);
  } catch (err) {
    console.error("❌ Error in notifyNearbyUsers:", err);
  }
}
