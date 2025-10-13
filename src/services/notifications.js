/**
 * /src/services/notifications.js
 * -------------------------------------------------------------
 * Centralized Firebase Cloud Messaging (FCM) notification service
 * for Disaster Help backend.
 *
 * Responsibilities:
 *   ✅ Register FCM tokens
 *   ✅ Send notifications to followers (geo-filtered)
 *   ✅ Notify followers of updates (comments, resolves, etc.)
 *   ✅ Clean up invalid tokens automatically
 *   ✅ Prevent duplicate notifications via collapse keys
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";
import { haversineDistanceMi } from "../utils/geoUtils.js";

// =============================================================
// 🔖 Register a user’s FCM token
// =============================================================

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

export async function notifyFollowers(collection, docId, title, body, data = {}) {
  try {
    const db = getDB();
    const coll = db.collection(collection);

    // ✅ Convert to ObjectId if valid
    const query =
      /^[0-9a-fA-F]{24}$/.test(docId)
        ? { _id: new ObjectId(docId) }
        : { _id: docId };

    const post = await coll.findOne(query);
    if (!post) {
      console.warn(`⚠️ notifyFollowers: document ${docId} not found in ${collection}`);
      return;
    }

    const followerIds = post.followers || [];
    if (followerIds.length === 0) {
      console.log(`ℹ️ No followers to notify for ${collection}/${docId}`);
      return;
    }

    // 🔍 Determine event location (for radius filtering)
    let eventLat = 0,
      eventLng = 0;
    if (post.geometry?.coordinates?.length >= 2) {
      [eventLng, eventLat] = post.geometry.coordinates;
    } else if (post.location?.coordinates?.length >= 2) {
      [eventLng, eventLat] = post.location.coordinates;
    } else {
      console.warn("⚠️ Event has no valid geometry, skipping geo filter.");
    }

    // 🔎 Fetch follower profiles
    const users = db.collection("users");
    const followerDocs = await users
      .find({ user_id: { $in: followerIds } })
      .project({ fcm_tokens: 1, lastLocation: 1, radiusMi: 1 })
      .toArray();

    const eligibleTokens = [];
    for (const u of followerDocs) {
      if (!u.fcm_tokens?.length || !u.lastLocation || !u.radiusMi) continue;

      const dist = haversineDistanceMi(
        u.lastLocation.lat,
        u.lastLocation.lng,
        eventLat,
        eventLng
      );

      if (!isNaN(dist) && dist <= u.radiusMi) {
        eligibleTokens.push(...u.fcm_tokens);
      }
    }

    if (eligibleTokens.length === 0) {
      console.log(`ℹ️ No followers within geofence for ${collection}/${docId}`);
      return;
    }

    // ✅ Add collapse keys to prevent duplicate system+local notifications
    const collapseId = `${collection}_${docId}`;

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${docId}`,
        collection,
        docId: docId.toString(),
        ...data,
      },
      android: {
        collapseKey: collapseId,
        priority: "high",
      },
      apns: {
        headers: {
          "apns-collapse-id": collapseId,
          "apns-priority": "10",
        },
      },
      tokens: eligibleTokens,
    };

    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `📤 Notification sent to ${eligibleTokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    await _cleanInvalidTokens(users, eligibleTokens, response);
  } catch (err) {
    console.error("❌ Error sending FCM notifications:", err);
  }
}

// =============================================================
// 🔔 Notify followers of an update (comments, resolve, etc.)
// =============================================================

export async function notifyFollowersOfUpdate(
  collection,
  docId,
  actorId,
  eventType,
  text = ""
) {
  try {
    const db = getDB();
    const coll = db.collection(collection);

    const query =
      /^[0-9a-fA-F]{24}$/.test(docId)
        ? { _id: new ObjectId(docId) }
        : { _id: docId };

    const post = await coll.findOne(query);
    if (!post) return console.warn(`⚠️ notifyFollowersOfUpdate: post ${docId} not found`);

    const followerIds = (post.followers || []).filter((id) => id !== actorId);
    if (followerIds.length === 0) return;

    const users = db.collection("users");
    const followerDocs = await users
      .find({ user_id: { $in: followerIds } })
      .project({ fcm_tokens: 1 })
      .toArray();

    const tokens = followerDocs
      .flatMap((u) => u.fcm_tokens || [])
      .filter((t) => typeof t === "string" && t.length > 10);

    if (tokens.length === 0) {
      console.log(`ℹ️ No active tokens for followers of ${collection}/${docId}`);
      return;
    }

    const title = `New ${eventType} on ${collection}`;
    const body = text || `${eventType} update on a post you follow`;

    const collapseId = `${collection}_${docId}_${eventType}`;

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${docId}`,
        collection,
        docId: docId.toString(),
        type: eventType,
      },
      android: {
        collapseKey: collapseId,
        priority: "high",
      },
      apns: {
        headers: {
          "apns-collapse-id": collapseId,
          "apns-priority": "10",
        },
      },
      tokens,
    };

    const res = await admin.messaging().sendEachForMulticast(message);
    console.log(`📢 Follower update notif: ${res.successCount}/${tokens.length} succeeded`);

    await _cleanInvalidTokens(users, tokens, res);
  } catch (err) {
    console.error("❌ notifyFollowersOfUpdate failed:", err);
  }
}

// =============================================================
// 🧹 Helper: Clean invalid tokens
// =============================================================

async function _cleanInvalidTokens(users, tokens, response) {
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
}
