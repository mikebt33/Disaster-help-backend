/**
 * /src/services/notifications.js
 * -------------------------------------------------------------
 * Centralized Firebase Cloud Messaging (FCM) notification service
 * for Disaster Help backend.
 *
 * Responsibilities:
 *   ✅ Use shared Firebase Admin instance (via firebaseAdmin.js)
 *   ✅ Register user FCM tokens
 *   ✅ Send notifications to all followers of a post
 *   ✅ Clean up invalid tokens automatically
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";

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
// 📣 Send notifications to followers of a post
// =============================================================

/**
 * Notify all followers of a post (help, offer, or hazard).
 *
 * @param {string} collection - "help_requests" | "offer_help" | "hazards"
 * @param {string|ObjectId} docId - Post/document ID
 * @param {string} title - Notification title
 * @param {string} body - Notification body text
 * @param {object} data - Extra payload (optional)
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

    const users = db.collection("users");
    const followerDocs = await users
      .find({ user_id: { $in: followerIds } })
      .project({ fcm_tokens: 1 })
      .toArray();

    const tokens = followerDocs
      .flatMap((u) => u.fcm_tokens || [])
      .filter((t) => typeof t === "string" && t.length > 10);

    if (tokens.length === 0) {
      console.log(`ℹ️ No active FCM tokens for followers of ${docId}`);
      return;
    }

    const message = {
      notification: { title, body },
      data: {
        deeplink: `disasterhelp://detail?c=${collection}&id=${docId}`,
        collection,
        docId: docId.toString(),
        ...data,
      },
      tokens,
    };

    const response = await admin.messaging().sendEachForMulticast(message);

    console.log(
      `📤 Notification sent to ${tokens.length} devices (success: ${response.successCount}, failed: ${response.failureCount})`
    );

    // Clean up invalid tokens
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
  } catch (err) {
    console.error("❌ Error sending FCM notifications:", err);
  }
}
