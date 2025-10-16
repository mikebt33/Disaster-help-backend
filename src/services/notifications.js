/**
 * /src/services/notifications.js
 * -------------------------------------------------------------
 * Followers notifications (updates) with collapse & token de-dup
 *   ✅ Excludes the actor (creator) by ID and tokens
 *   ✅ Includes senderId in data payload for client-side filtering
 *   ✅ Collapses duplicate notifications by action/doc
 *   ✅ Cleans invalid tokens automatically
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
      {
        $addToSet: { fcm_tokens: token },
        $setOnInsert: { createdAt: new Date() },
      },
      { upsert: true }
    );
    return { message: "✅ FCM token registered successfully." };
  } catch (err) {
    console.error("❌ registerFcmToken:", err);
    return { error: "Internal server error" };
  }
}

// 🔔 Notify followers of an update (confirm/dispute/comment/resolve/follow)
export async function notifyFollowersOfUpdate(
  collection,
  docId,
  actorUserId,
  eventType,
  text = ""
) {
  try {
    const db = getDB();
    const coll = db.collection(collection);
    const query =
      /^[0-9a-fA-F]{24}$/.test(String(docId))
        ? { _id: new ObjectId(String(docId)) }
        : { _id: String(docId) };

    const post = await coll.findOne(query);
    if (!post) {
      console.warn(`[PUSH][follow] ⚠️ ${collection}/${docId} not found`);
      return;
    }

    const followerIds = (post.followers || []).map(String);
    if (followerIds.length === 0) {
      console.log(`[PUSH][follow] ${collection}/${docId} has no followers`);
      return;
    }

    const users = db.collection("users");

    // Fetch all follower user docs
    const followerUsers = await users
      .find({
        user_id: { $in: followerIds },
        fcm_tokens: { $exists: true, $ne: [] },
      })
      .project({ user_id: 1, fcm_tokens: 1 })
      .toArray();

    // Fetch actor's tokens (to exclude)
    const actor =
      actorUserId &&
      (await users.findOne(
        { user_id: String(actorUserId) },
        { projection: { fcm_tokens: 1 } }
      ));
    const actorTokens = new Set(
      Array.isArray(actor?.fcm_tokens)
        ? actor.fcm_tokens.filter((t) => typeof t === "string" && t.length > 10)
        : []
    );

    // Build token set excluding actor
    const tokenSet = new Set();
    for (const u of followerUsers) {
      if (u.user_id && String(u.user_id) === String(actorUserId)) continue;
      for (const t of u.fcm_tokens || []) {
        if (typeof t === "string" && t.length > 10 && !actorTokens.has(t)) {
          tokenSet.add(t);
        }
      }
    }

    const tokens = Array.from(tokenSet);
    if (tokens.length === 0) {
      console.log(`[PUSH][follow] ℹ️ No follower tokens for ${collection}/${docId}`);
      return;
    }

    // Human-readable action title
    const labelMap = {
      confirm: "👍 Confirmed",
      dispute: "👎 Disputed",
      comment: "💬 Commented",
      resolve: "✅ Resolved",
      follow: "👀 Followed",
    };
    const emoji = labelMap[eventType] || "📍";
    const title = `${emoji} Update on ${collection.replace("_", " ")}`;
    const body =
      text && text.length > 0
        ? text
        : `A ${collection.replace("_", " ")} you follow was ${eventType}.`;

    const collapseKey = `fup_${collection}_${String(docId)}_${eventType}`;

    // --- Construct message
    const message = {
      notification: { title, body },
      data: {
        senderId: actorUserId ? String(actorUserId) : "",
        action: String(eventType || ""),
        collection,
        docId: String(docId),
        deeplink: `disasterhelp://detail?c=${collection}&id=${String(docId)}`,
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
    console.log(
      `[PUSH][follow] 📤 ${collection}/${docId} ${eventType} -> ${res.successCount}/${tokens.length} ok`
    );

    // --- Clean invalid tokens
    const invalid = [];
    res.responses.forEach((r, i) => {
      if (!r.success) {
        const code = r.error?.code || "unknown";
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          invalid.push(tokens[i]);
        } else {
          console.warn("[PUSH][follow] send error:", code);
        }
      }
    });
    if (invalid.length > 0) {
      await users.updateMany(
        { fcm_tokens: { $in: invalid } },
        { $pull: { fcm_tokens: { $in: invalid } } }
      );
      console.log(`[PUSH][follow] 🧹 Removed ${invalid.length} invalid tokens.`);
    }
  } catch (err) {
    console.error("❌ notifyFollowersOfUpdate:", err);
  }
}
