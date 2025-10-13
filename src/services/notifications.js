/**
 * /src/services/notifications.js
 * -------------------------------------------------------------
 * Followers notifications (updates) with collapse & token de-dup
 * -------------------------------------------------------------
 */

import admin from "./firebaseAdmin.js";
import { getDB } from "../db.js";
import { ObjectId } from "mongodb";

// 🔖 Register FCM token (unchanged)
export async function registerFcmToken(userId, token) {
  if (!userId || !token) return { error: "Missing userId or token" };
  try {
    const db = getDB();
    const users = db.collection("users");
    await users.updateOne(
      { user_id: userId },
      { $addToSet: { fcm_tokens: token }, $setOnInsert: { createdAt: new Date() } },
      { upsert: true }
    );
    return { message: "✅ FCM token registered successfully." };
  } catch (err) {
    console.error("❌ registerFcmToken:", err);
    return { error: "Internal server error" };
  }
}

// 🔔 Notify followers of an update (comments, resolve, etc.)
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
      /^[0-9a-fA-F]{24}$/.test(docId) ? { _id: new ObjectId(docId) } : { _id: docId };

    const post = await coll.findOne(query);
    if (!post) {
      console.warn(`⚠️ notifyFollowersOfUpdate: ${collection}/${docId} not found`);
      return;
    }

    // Exclude the actor
    const followerIds = (post.followers || []).filter((id) => id !== actorId);
    if (followerIds.length === 0) return;

    const users = db.collection("users");
    const followerDocs = await users
      .find({ user_id: { $in: followerIds } })
      .project({ fcm_tokens: 1 })
      .toArray();

    // De-dup tokens
    const tokenSet = new Set();
    for (const u of followerDocs) {
      for (const t of (u.fcm_tokens || [])) {
        if (typeof t === "string" && t.length > 10) tokenSet.add(t);
      }
    }
    const tokens = Array.from(tokenSet);
    if (tokens.length === 0) return;

    const title = `New ${eventType} on ${collection}`;
    const body = text || `${eventType} update on a post you follow`;
    const collapseKey = `fup_${collection}_${String(docId)}_${eventType}`;

    const message = {
      notification: { title, body },
      data: {
        title,
        body,
        deeplink: `disasterhelp://detail?c=${collection}&id=${String(docId)}`,
        collection,
        docId: String(docId),
        type: eventType,
      },
      tokens,
      android: {
        priority: "high",
        collapseKey,
        notification: { channelId: "alerts", tag: collapseKey },
      },
      apns: {
        headers: {
          "apns-priority": "10",
          "apns-collapse-id": collapseKey,
        },
        payload: { aps: { sound: "default" } },
      },
    };

    const res = await admin.messaging().sendEachForMulticast(message);
    console.log(`📢 Followers update: ${res.successCount}/${tokens.length} sent`);

    // Clean invalid
    const invalid = [];
    res.responses.forEach((r, i) => {
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
      console.log(`🧹 Removed ${invalid.length} invalid tokens (followers).`);
    }
  } catch (err) {
    console.error("❌ notifyFollowersOfUpdate:", err);
  }
}
