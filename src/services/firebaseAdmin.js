// src/services/firebaseAdmin.js
import admin from "firebase-admin";
import { readFileSync, existsSync } from "fs";
import path from "path";

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
      const keyPath = path.resolve("./keys/fcm-service-account.json");
      if (existsSync(keyPath)) {
        const creds = JSON.parse(readFileSync(keyPath, "utf8"));
        admin.initializeApp({
          credential: admin.credential.cert(creds),
        });
        console.log("✅ Firebase Admin initialized with local service account file");
      } else {
        admin.initializeApp({
          credential: admin.credential.applicationDefault(),
        });
        console.log("⚠️ Firebase Admin initialized using ADC fallback");
      }
    }
  } catch (err) {
    console.error("❌ Firebase Admin initialization failed:", err);
  }
}

export default admin;
