/**
 * /src/routes/user.js
 * -------------------------------------------------------------
 * REST endpoints for user-related operations:
 *   ✅ Register or update FCM tokens
 *   ✅ Manually trigger follower notifications
 * -------------------------------------------------------------
 */

import express from "express";
import { registerFcmToken } from "../services/notifications.js";

const router = express.Router();

/**
 * POST /api/user/register-token
 * Registers or updates an FCM token for a given device/user.
 * Expects JSON body: { "user_id": "abc123", "fcm_token": "token_here" }
 */
router.post("/register-token", async (req, res) => {
  try {
    const { user_id, fcm_token } = req.body;
    if (!user_id || !fcm_token) {
      return res
        .status(400)
        .json({ error: "user_id and fcm_token are required." });
    }

    const result = await registerFcmToken(user_id, fcm_token);
    if (result.error) {
      return res.status(500).json(result);
    }

    res.json(result);
  } catch (err) {
    console.error("❌ Error in /register-token:", err);
    res.status(500).json({ error: "Internal server error." });
  }
});

/**
 * POST /api/user/notify-followers
 * Sends a push notification to all followers of a given post.
 * Expects JSON:
 *   {
 *     "collection": "hazards",
 *     "docId": "68e92423ba99259ddfb2e017",
 *     "title": "Severe Weather Update",
 *     "message": "Flash flood warning extended"
 *   }
 */
router.post("/notify-followers", async (req, res) => {
  try {
    const { collection, docId, title, message } = req.body;
    if (!collection || !docId || !title || !message) {
      return res.status(400).json({
        error: "collection, docId, title, and message are required.",
      });
    }

    await notifyFollowers(collection, docId, title, message);
    res.json({ message: "✅ Notifications dispatched (check logs for results)" });
  } catch (err) {
    console.error("❌ Error in /notify-followers:", err);
    res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
