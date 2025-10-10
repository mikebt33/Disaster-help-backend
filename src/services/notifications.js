/**
 * /src/services/notifications.js
 * -------------------------------------------------------------
 * Centralized Firebase Cloud Messaging (FCM) notification service
 * for Disaster Help backend.
 *
 * Responsibilities:
 *   ✅ Use shared Firebase Admin instance (via firebaseAdmin.js)
 *   ✅ Register user FCM tokens
 *   ✅ Send notifications to followers of a post WITH geofence filtering
 *   ✅ Clean up invalid tokens automatically
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { haversineDistanceMi } from "../utils/geoUtils.js"; // ✅ distance helper

// =============================================================
// 🔖 Register a user’s FCM token
// =============================================================

/**
 * Save or update a user’s FCM token.
 * Creates user record if missing.
 */
export async function registerFcmToken(userId, token) {
  if (!userId || !token) {
    console.warn("⚠️ Missing userId or token in registerFcmToken");
    return { error: "Missing userId or token" };
  }

  try {
    const db = getDB();
    const users = db.collection("users");

    await users.updateOne(
      { user_id: userId },
      {
        $addToSet: { fcm_tokens: token },
        $setOnInsert: { createdAt: new Date() },
      },
      { upsert: true }
    );

    console.log(`💾 Registered FCM token for user ${userId}`);
    return { message: "✅ FCM token registered successfully." };
  } catch (err) {
    console.error("❌ Error registering FCM token:", err);
    return { error: "Internal server error" };
  }
}

// =============================================================
// 📣 Send notifications to followers (geo-filtered)
// =============================================================

/**
 * Notify all followers of a post (help, offer, or hazard),
 * but only if the event is within their configured radiusMi.
 *
 * Each follower document must contain:
 *   {
 *     user_id,
 *     fcm_tokens: [ ... ],
 *     lastLocation: { lat, lng },
 *     radiusMi: number
 *   }
 */
export async function notifyFollowers(collection, docId, title, body, data = {}) {
  try {
    const db = getDB();
    const coll = db.collection(collection);
    const post = await coll.findOne({ _id: docId });

    if (!post) {
      console.warn(`⚠️ notifyFollowers: document ${docId} not found in ${collection}`);
      return;
    }

    const followerIds = post.followers || [];
    if (followerIds.length === 0) {
      console.log(`ℹ️ No followers to notify for ${collection}/${docId}`);
      return;
    }

    // 🔍 Determine event location (hazards.geometry / help_requests.location)
    let eventLat = 0, eventLng = 0;
    if (post.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = post.geometry.coordinates;
    } else if (post.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = post.location.coordinates;
    } else {
      console.warn("⚠️ Event has no valid location geometry, skipping geo filter.");
    }

    // 🔎 Get all follower user profiles
    const users = db.collection("users");
    const followerDocs = await users
      .find({ user_id: { $in: followerIds } })
      .project({ fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    const eligibleTokens = [];
    for (const u of followerDocs) {
      if (!u.fcm_tokens?.length || !u.lastLocation || !u.radiusMi) continue;

      // 🧮 Calculate distance
      const dist = haversineDistanceMi(
        u.lastLocation.lat,
        u.lastLocation.lng,
        eventLat,
        eventLng
      );

      if (isNaN(dist)) continue;
      if (dist <= u.radiusMi) {
        eligibleTokens.push(...u.fcm_tokens);
      }
    }

    if (eligibleTokens.length === 0) {
      console.log(`ℹ️ No followers within geofence for ${collection}/${docId}`);
      return;
    }

    // 📨 Prepare and send
    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${docId}`,
        collection,
        docId: docId.toString(),
        ...data,
      },
      tokens: eligibleTokens,
    };

    const response = await admin.messaging().sendEachForMulticast(message);

    console.log(
      `📤 Notification sent to ${eligibleTokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
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
          invalidTokens.push(eligibleTokens[i]);
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
    console.error("❌ Error sending FCM notifications:", err);
  }
}
