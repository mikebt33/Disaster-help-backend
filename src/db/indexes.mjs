import { getDB } from "../db.js";
export async function ensureIndexes() {
  const db = getDB();
  const col = db.collection("social_signals");
  await col.createIndex({ expires: 1 }, { expireAfterSeconds: 0 });
  await col.createIndex({ geometry: "2dsphere" });
  await col.createIndex({ url: 1 }, { unique: true });
}
