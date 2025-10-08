import { getDB } from "../db.js";

/**
 * Cleanup Service
 * - Deletes help_requests, offer_help, and hazards older than 72 hours
 * - Deletes CAP alerts that have expired (based on their `info.expires` field)
 */
export async function runCleanup() {
  try {
    console.log("🧹 Running daily cleanup...");

    const db = getDB();
    const now = new Date();
    const cutoff = new Date(now.getTime() - 72 * 60 * 60 * 1000); // 72 hours ago

    // Collections
    const helpRequests = db.collection("help_requests");
    const offers = db.collection("offer_help");
    const hazards = db.collection("hazards");
    const alerts = db.collection("alerts_cap");

    // 1️⃣ Delete old user data (help_requests, offers, hazards)
    const helpDel = await helpRequests.deleteMany({ timestamp: { $lt: cutoff } });
    const offerDel = await offers.deleteMany({ timestamp: { $lt: cutoff } });
    const hazardDel = await hazards.deleteMany({
      timestamp: { $lt: cutoff },
      source: { $ne: "CAP" } // Keep CAP-related hazards
    });

    // 2️⃣ Delete expired CAP alerts
    // Some CAP feeds store expiration under info.expires, others under expires directly
    const capDel = await alerts.deleteMany({
      $or: [
        { "info.expires": { $lt: now } },
        { expires: { $lt: now } }
      ]
    });

    console.log(
      `✅ Cleanup complete:
      • Help requests deleted: ${helpDel.deletedCount}
      • Offers deleted: ${offerDel.deletedCount}
      • Hazards deleted: ${hazardDel.deletedCount}
      • Expired CAP alerts deleted: ${capDel.deletedCount}`
    );
  } catch (err) {
    console.error("❌ Cleanup error:", err.message);
  }
}
