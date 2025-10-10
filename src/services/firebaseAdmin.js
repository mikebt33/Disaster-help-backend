// src/services/firebaseAdmin.js
import admin from "firebase-admin";

if (!admin.apps.length) {
  try {
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (raw) {
      const credentials = JSON.parse(raw);
      admin.initializeApp({
        credential: admin.credential.cert(credentials),
      });
      console.log("✅ Firebase Admin initialized with inline credentials");
    } else {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
      });
      console.log("⚠️ Firebase Admin initialized using ADC (fallback)");
    }
  } catch (err) {
    console.error("❌ Firebase Admin init failed:", err);
  }
}

export default admin;
