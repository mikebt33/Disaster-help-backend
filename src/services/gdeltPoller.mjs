// src/services/gdeltPoller.mjs
// GDELT poller: ingests latest US negative/high-impact events into social_signals.

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

// Helper: parse GDELT SQLDATE (YYYYMMDD) → JS Date (UTC midnight)
function parseGdeltDate(sqlDate) {
  if (!sqlDate || sqlDate.length !== 8) return new Date();
  const y = sqlDate.slice(0, 4);
  const m = sqlDate.slice(4, 6);
  const d = sqlDate.slice(6, 8);
  return new Date(`${y}-${m}-${d}T00:00:00Z`);
}

// Helper: safely extract domain from URL
function getDomain(url) {
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  try {
    // 1) Get the latest update file (points to the newest event ZIPs)
    const { data: lastFileText } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
    });

    // Find the latest events ZIP (lines usually contain ...export.CSV.zip)
    const lines = String(lastFileText).split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => l.includes(".export.CSV.zip"));
    if (!zipLine) {
      console.warn("⚠️ GDELT: no export CSV ZIP found in lastupdate.txt");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ GDELT downloading:", zipUrl);

    // 2) Download the ZIP as a stream
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
    });

    // 3) Find the CSV entry inside the ZIP
    const directory = zipResp.data.pipe(unzipper.Parse({ forceStream: true }));
    let csvStream = null;

    for await (const entry of directory) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry;
        break;
      } else {
        entry.autodrain();
      }
    }

    if (!csvStream) {
      console.warn("⚠️ GDELT: no CSV entry found in ZIP");
      return;
    }

    console.log("📄 GDELT parsing CSV stream…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let countSeen = 0;
    let countSaved = 0;

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      const cols = line.split("\t");
      // Basic sanity check – GDELT events CSV has 58+ cols
      if (cols.length < 58) continue;

      // Column indices (GDELT 2.0 Events)
      const sqlDate = cols[1];              // SQLDATE
      const eventCode = cols[26];           // EventCode
      const eventBaseCode = cols[27];       // EventBaseCode
      const eventRootCode = cols[28];       // EventRootCode
      const quadClass = cols[29];           // QuadClass
      const goldstein = parseFloat(cols[30]); // GoldsteinScale
      const avgTone = parseFloat(cols[34]);  // AvgTone

      // Use ActionGeo_* as the location of the event
      const actionGeoFullName = cols[50];   // ActionGeo_FullName
      const actionGeoCountry = cols[51];    // ActionGeo_CountryCode (FIPS)
      const actionGeoLat = parseFloat(cols[53]);
      const actionGeoLon = parseFloat(cols[54]);

      const url = cols[57];                 // SOURCEURL

      // ---- Filters ----

      // Only United States events (FIPS code "US")
      if (actionGeoCountry !== "US") continue;

      // Require usable geometry
      if (isNaN(actionGeoLat) || isNaN(actionGeoLon)) continue;

      // Keep only clearly negative / impactful events
      // (GoldsteinScale strongly negative OR AvgTone notably negative)
      if (!(goldstein <= -5 || avgTone <= -3)) continue;

      countSeen++;

      const publishedAt = parseGdeltDate(sqlDate);
      const domain = getDomain(url);

      const titleBase = actionGeoFullName || "GDELT Event";
      const title = `${titleBase} (${eventCode || "event"})`;

      const doc = {
        // Keep same shape as NewsAPI docs where possible
        type: "news",
        source: "GDELT",
        domain,
        url,
        title,
        description: `GDELT-coded event at ${actionGeoFullName || "unknown location"}`,
        publishedAt,
        createdAt: new Date(),
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000), // 24h TTL via indexes.mjs

        // GeoJSON geometry
        geometry: {
          type: "Point",
          coordinates: [actionGeoLon, actionGeoLat],
        },
        geometryMethod: "gdelt-action-geo",

        // GDELT-specific metadata
        gdelt: {
          sqlDate,
          eventCode,
          eventBaseCode,
          eventRootCode,
          quadClass,
          goldstein,
          avgTone,
          locationText: actionGeoFullName,
        },
      };

      if (!url) continue;

      // Upsert by URL
      await col.updateOne(
        { url },
        { $set: doc },
        { upsert: true }
      );
      countSaved++;
    }

    console.log(`🌎 GDELT scanned ${countSeen} US negative events, saved ${countSaved}.`);
  } catch (err) {
    if (err.response) {
      console.error("❌ GDELT HTTP error:", err.response.status, err.response.data);
    } else {
      console.error("❌ GDELT poll error:", err.message);
    }
  }
}
