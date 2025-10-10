/**
 * /src/services/notifyNearbyUsers.js
 * -------------------------------------------------------------
 * Centralized geofence-based notification logic.
 * Triggers when a new hazard/help/offer post is created.
 *
 * Responsibilities:
 *   ✅ Load all users with location + radiusMi
 *   ✅ Compute distance using Haversine formula
 *   ✅ Send FCM notifications to those inside the radius
 *   ✅ Clean invalid tokens automatically
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js";

/**
 * Notify all users within their configured radius of a new event.
 *
 * @param {string} collection - "hazards" | "help_requests" | "offer_help"
 * @param {object} doc - The new document that was inserted
 */
export async function notifyNearbyUsers(collection, doc) {
  try {
    const db = getDB();
    const users = db.collection("users");

    // 🔍 Extract event location
    let eventLat = 0,
      eventLng = 0;
    if (doc.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.geometry.coordinates;
    } else if (doc.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = doc.location.coordinates;
    } else {
      console.warn("⚠️ notifyNearbyUsers: Event missing geometry/location");
      return;
    }

    // 🧭 Fetch all users with location + radius
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

    const eligible = [];
    for (const u of candidates) {
      if (!u.lastLocation || !u.radiusMi) continue;

      const dist = haversineDistanceMi(
        u.lastLocation.lat,
        u.lastLocation.lng,
        eventLat,
        eventLng
      );

      if (!isNaN(dist) && dist <= u.radiusMi) {
        eligible.push(...(u.fcm_tokens || []));
      }
    }

    if (eligible.length === 0) {
      console.log(`ℹ️ No nearby users to notify for new ${collection} post.`);
      return;
    }

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

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${doc._id}`,
        collection,
        docId: doc._id.toString(),
      },
      tokens: eligible,
    };

    const response = await admin.messaging().sendEachForMulticast(message);

    console.log(
      `📤 Geo-notification sent to ${eligible.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    // 🧹 Clean up invalid tokens
    const invalidTokens = [];
    response.responses.forEach((r, i) => {
      if (!r.success) {
        const code = r.error?.code || "unknown";
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          invalidTokens.push(eligible[i]);
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
  } catch (err) {
    console.error("❌ Error in notifyNearbyUsers:", err);
  }
}
