// src/routes/user.js
import express from "express";
import { getDB } from "../db.js";
import admin from "../services/firebaseAdmin.js";
import { registerFcmToken } from "../services/notifications.js";

const router = express.Router();

/**
 * POST /api/user/register-token
 * body: { user_id: string, fcm_token: string }
 */
router.post("/register-token", async (req, res) => {
  try {
    const { user_id, fcm_token } = req.body || {};
    if (!user_id || !fcm_token) {
      return res.status(400).json({ error: "user_id and fcm_token are required" });
    }
    const r = await registerFcmToken(user_id, fcm_token);
    if (r?.error) return res.status(500).json(r);
    return res.json({ ok: true });
  } catch (e) {
    console.error("❌ register-token failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * ✅ PUT /api/user/location
 * body: { user_id: string, lat: number, lng: number, radius_mi?: number }
 * Stores lastLocation for geofencing and distance filters.
 */
router.put("/location", async (req, res) => {
  try {
    const { user_id, lat, lng, radius_mi } = req.body || {};
    if (!user_id || typeof lat !== "number" || typeof lng !== "number") {
      return res.status(400).json({ error: "user_id, lat, and lng are required" });
    }

    const db = getDB();
    const update = {
      $set: {
        lastLocation: { lat, lng },
        radius_mi: radius_mi ?? 10,
        updatedAt: new Date(),
      },
      $setOnInsert: { createdAt: new Date() },
    };

    const result = await db.collection("users").updateOne({ user_id }, update, {
      upsert: true,
    });

    console.log(`[User] ✅ Location updated for ${user_id} (${lat}, ${lng})`);
    return res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount });
  } catch (e) {
    console.error("❌ /location failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * PATCH /api/user/settings
 * body: { user_id: string, radiusMi?: number, notificationsEnabled?: boolean }
 */
router.patch("/settings", async (req, res) => {
  try {
    const { user_id, radiusMi, notificationsEnabled } = req.body || {};
    if (!user_id) return res.status(400).json({ error: "user_id is required" });

    const update = {};
    if (typeof radiusMi === "number") update["radiusMi"] = radiusMi;
    if (typeof notificationsEnabled === "boolean")
      update["notificationsEnabled"] = notificationsEnabled;

    if (Object.keys(update).length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }

    const db = getDB();
    await db.collection("users").updateOne(
      { user_id },
      { $set: { ...update, updatedAt: new Date() } },
      { upsert: true }
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("❌ /settings failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/user/me?user_id=abc
 * Returns minimal profile for debugging geofence + push.
 */
router.get("/me", async (req, res) => {
  try {
    const { user_id } = req.query;
    if (!user_id) return res.status(400).json({ error: "user_id is required" });

    const db = getDB();
    const u = await db
      .collection("users")
      .findOne({ user_id }, { projection: { _id: 0 } });

    res.json(u || {});
  } catch (e) {
    console.error("❌ /me failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/user/test-push
 * body: { token?: string, user_id?: string, title?, body? }
 * Sends a test push to a token or to all tokens of a user.
 */
router.post("/test-push", async (req, res) => {
  try {
    const { token, user_id, title, body } = req.body || {};
    let tokens = [];

    const db = getDB();

    if (token) {
      tokens = [token];
    } else if (user_id) {
      const u = await db.collection("users").findOne(
        { user_id },
        { projection: { fcm_tokens: 1 } }
      );
      tokens = (u?.fcm_tokens || []).filter(
        (t) => typeof t === "string" && t.length > 10
      );
    } else {
      return res.status(400).json({ error: "Provide token or user_id" });
    }

    if (tokens.length === 0) {
      return res.status(400).json({ error: "No valid tokens found" });
    }

    const message = {
      notification: {
        title: title || "Test Notification",
        body: body || "This is a test push from /api/user/test-push",
      },
      data: { deeplink: "disasterhelp://home" },
      tokens,
      android: {
        priority: "high",
        notification: { channelId: "alerts" },
      },
      apns: {
        headers: { "apns-priority": "10" },
        payload: { aps: { sound: "default" } },
      },
    };

    const resp = await admin.messaging().sendEachForMulticast(message);

    console.log(`[PushTest] Sent to ${tokens.length}, success=${resp.successCount}`);
    res.json({
      successCount: resp.successCount,
      failureCount: resp.failureCount,
      errors: resp.responses
        .map((r, i) =>
          !r.success ? { token: tokens[i], code: r.error?.code } : null
        )
        .filter(Boolean),
    });
  } catch (e) {
    console.error("❌ /test-push failed:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
