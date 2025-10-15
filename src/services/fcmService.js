// src/services/fcmService.js
import admin from "./firebaseAdmin.js";

const messaging = admin.messaging();

/**
 * Sends a Firebase Cloud Message (FCM) to a specific device.
 * @param {string} token - Device FCM registration token.
 * @param {string} title - Notification title.
 * @param {string} body - Notification body.
 * @param {object} data - Optional key-value data payload.
 */
export async function sendPush(token, title, body, data = {}) {
  if (!token) {
    console.warn("⚠️ No FCM token provided to sendPush()");
    return;
  }

  const message = {
    token,
    notification: { title, body },
    data,
  };

  try {
    const response = await messaging.send(message);
    console.log("✅ FCM message sent:", response);
    return response;
  } catch (err) {
    console.error("❌ FCM send error:", err.message || err);
  }
}
