/**
 * /src/services/notifyNearbyUsers.js
 * -------------------------------------------------------------
 * Geo-fence notifications when a new hazard/help/offer is created.
 * Now with:
 *   ✅ DB idempotency guard (works across processes)
 *   ✅ Token Set de-duplication
 *   ✅ Exclude creator
 *   ✅ collapseKey / tag / apns-collapse-id
 *   ✅ Invalid token cleanup
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js";
import { shouldSendNow } from "./sendOnce.js";

/**
 * @param {"hazards"|"help_requests"|"offer_help"} collection
 * @param {object} doc  - must include _id and geometry/location
 * @param {object} [opts]
 * @param {string} [opts.excludeUserId] - usually the creator
 */
export async function notifyNearbyUsers(collection, doc, opts = {}) {
  try {
    const db = getDB();
    const users = db.collection("users");

    // ---- Idempotency guard (DB-level, TTL) --------------------
    const sendKey = `geo:${collection}:${String(doc._id)}`;
    const ok = await shouldSendNow(sendKey, 60_000); // 60s window
    if (!ok) {
      console.log(`⏩ Skipping duplicate geo send for ${sendKey}`);
      return;
    }

    // ---- Extract event location ------------------------------
    let eventLat = 0, eventLng = 0;
    if (doc?.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.geometry.coordinates;
    } else if (doc?.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.location.coordinates;
    } else if (typeof doc.lat === "number" && typeof doc.lng === "number") {
      eventLat = doc.lat; eventLng = doc.lng;
    } else {
      console.warn("⚠️ notifyNearbyUsers: Event missing geometry/location.");
      return;
    }

    // ---- Find candidate users with tokens & radius ------------
    const candidates = await users
      .find({
        lastLocation: { $exists: true },
        radiusMi: { $gt: 0 },
        fcm_tokens: { $exists: true, $ne: [] },
      })
      .project({ user_id: 1, fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    if (!candidates.length) {
      console.log("ℹ️ No users with location + tokens found.");
      return;
    }

    // ---- Build token set (dedupe) & exclude creator ----------
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
      console.log(`ℹ️ No nearby users to notify for ${collection}/${doc._id}.`);
      return;
    }

    // ---- Compose message -------------------------------------
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

    const collapseKey = `geo_${collection}_${String(doc._id)}`;

    const message = {
      // Keep notification block so OS shows it when app is background/killed
      notification: { title, body },
      data: {
        // Also include in data for foreground/local handling & deep link
        title,
        body,
        deeplink: `disasterhelp://detail?c=${collection}&id=${String(doc._id)}`,
        collection,
        docId: String(doc._id),
      },
      tokens,
      android: {
        priority: "high",
        collapseKey,
        notification: {
          channelId: "alerts",
          tag: collapseKey, // coalesce duplicates in tray
        },
      },
      apns: {
        headers: {
          "apns-priority": "10",
          "apns-collapse-id": collapseKey, // coalesce on iOS
        },
        payload: { aps: { sound: "default" } },
      },
    };

    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `📤 Geo-notif ${collection}/${doc._id}: ${response.successCount}/${tokens.length} succeeded`
    );

    // ---- Clean invalid tokens --------------------------------
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
  } catch (err) {
    console.error("❌ notifyNearbyUsers error:", err);
  }
}
