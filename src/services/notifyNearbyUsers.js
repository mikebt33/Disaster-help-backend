/**
 * /src/services/notifyNearbyUsers.js
 * -------------------------------------------------------------
 * Centralized geofence-based notification logic.
 * Triggers when a new hazard/help/offer post is created.
 *
 * Improvements in this version:
 *   ✅ Deduplicate FCM tokens across all matched users
 *   ✅ Skip notifying the creator (optional)
 *   ✅ Event-level send guard (TTL) to avoid double-sends
 *   ✅ Android collapseKey + notification.tag to coalesce duplicates
 *   ✅ iOS apns-collapse-id to coalesce duplicates
 *   ✅ Clean invalid tokens automatically
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js";

// Simple in-process send guard to avoid sending the *same* event twice
// If you run multiple server instances, move this to Redis.
const recentlySent = new Map(); // key -> timestamp
const TTL_MS = 60_000;          // 60s dedupe window

function _markSent(key) {
  recentlySent.set(key, Date.now());
  // cleanup later to keep map small
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
  try {
    const db = getDB();
    const users = db.collection("users");

    // Dedupe guard per event
    const sendKey = `geo:${collection}:${doc._id}`;
    const last = recentlySent.get(sendKey);
    if (last && Date.now() - last < TTL_MS) {
      console.log(`⏩ Skipping duplicate send for ${sendKey}`);
      return;
    }

    // 🔎 Extract event location
    let eventLat = 0, eventLng = 0;
    if (doc?.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.geometry.coordinates;
    } else if (doc?.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.location.coordinates;
    } else if (typeof doc.lat === "number" && typeof doc.lng === "number") {
      eventLat = doc.lat; eventLng = doc.lng;
    } else {
      console.warn("⚠️ notifyNearbyUsers: Event missing geometry/location");
      return;
    }

    // 🧭 Fetch all users with location + radius + at least one token
    const candidates = await users
      .find({
        lastLocation: { $exists: true },
        radiusMi: { $gt: 0 },
        fcm_tokens: { $exists: true, $ne: [] },
      })
      .project({ user_id: 1, fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    if (!candidates.length) {
      console.log("ℹ️ No users with location or FCM tokens found.");
      return;
    }

    // ✅ Build a unique token set (dedupe across all user docs)
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
        for (const t of (u.fcm_tokens || [])) {
          if (typeof t === "string" && t.length > 10) tokenSet.add(t);
        }
      }
    }

    const tokens = Array.from(tokenSet);
    if (tokens.length === 0) {
      console.log(`ℹ️ No nearby users to notify for new ${collection} post.`);
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

    const collapseKey = `geo_${collection}_${doc._id}`; // same key coalesces duplicates on device

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${doc._id}`,
        collection,
        docId: String(doc._id),
      },
      tokens,
      android: {
        priority: "high",
        collapseKey,
        notification: {
          channelId: "alerts",        // match your Android channel
          tag: collapseKey,           // coalesce in the notification tray
        },
      },
      apns: {
        headers: {
          "apns-priority": "10",
          "apns-collapse-id": collapseKey, // coalesce on iOS
        },
        payload: {
          aps: { sound: "default" },
        },
      },
    };

    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `📤 Geo-notification sent to ${tokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    // 🧹 Clean invalid tokens
    const invalid = [];
    response.responses.forEach((r, i) => {
      if (!r.success) {
        const code = r.error?.code || "unknown";
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          invalid.push(tokens[i]);
        }
      }
    });

    if (invalid.length > 0) {
      await users.updateMany(
        { fcm_tokens: { $in: invalid } },
        { $pull: { fcm_tokens: { $in: invalid } } }
      );
      console.log(`🧹 Removed ${invalid.length} invalid tokens.`);
    }

    // Mark this event sent (dedupe window)
    _markSent(sendKey);
  } catch (err) {
    console.error("❌ Error in notifyNearbyUsers:", err);
  }
}
