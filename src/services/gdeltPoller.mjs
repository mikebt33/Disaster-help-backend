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
    // 1) Get the latest update file (points to newest ZIPs)
    const { data: lastFileText } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
    });

    const lines = String(lastFileText).split(/\r?\n/).filter(Boolean);

    // Pick the EVENTS export file: YYYYMMDDHHMMSS.export.CSV.zip
    const zipLine = lines.find((l) =>
      l.match(/\/\d{14}\.export\.CSV\.zip$/i)
    );

    if (!zipLine) {
      console.warn("⚠️ GDELT: no Events .export.CSV.zip found in lastupdate.txt");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ GDELT downloading:", zipUrl);

    // 2) Download ZIP
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
    });

    // 3) Extract CSV from ZIP
    const directory = zipResp.data.pipe(unzipper.Parse({ forceStream: true }));
    let csvStream = null;

    for await (const entry of directory) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry;
        break;
      }
      entry.autodrain();
    }

    if (!csvStream) {
      console.warn("⚠️ GDELT: no CSV entry found in ZIP");
      return;
    }

    console.log("📄 GDELT parsing CSV stream…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let debugRawPrinted = 0;
    let countMatched = 0;
    let countSaved = 0;

    for await (const line of rl) {
      if (!line.trim()) continue;

      const cols = line.split("\t");
      // GDELT 2.0 Events CSV has 61 columns (0–60)
      if (cols.length < 61) {
        // Not an Events row (could be malformed or another file type)
        continue;
      }

      // Column indices (GDELT 2.0 EVENTS)
      //  1: SQLDATE
      // 26: EventCode
      // 27: EventBaseCode
      // 28: EventRootCode
      // 29: QuadClass
      // 30: GoldsteinScale
      // 34: AvgTone
      // 52: ActionGeo_FullName
      // 53: ActionGeo_CountryCode
      // 56: ActionGeo_Lat
      // 57: ActionGeo_Long
      // 60: SOURCEURL
      const sqlDate = cols[1];
      const eventCode = cols[26];
      const eventBaseCode = cols[27];
      const eventRootCode = cols[28];
      const quadClass = cols[29];
      const goldstein = parseFloat(cols[30]);
      const avgTone = parseFloat(cols[34]);

      const actionGeoFullName = cols[52];
      const actionGeoCountry = cols[53];
      const actionGeoLat = parseFloat(cols[56]);
      const actionGeoLon = parseFloat(cols[57]);
      const url = cols[60];

      // --------------------
      // DEBUG: first 20 raw rows
      // --------------------
      if (debugRawPrinted < 20) {
        console.log("GDELT RAW:", {
          sqlDate,
          eventCode,
          eventRootCode,
          goldstein,
          avgTone,
          url,
          geoName: actionGeoFullName,
          country: actionGeoCountry,
          lat: actionGeoLat,
          lon: actionGeoLon,
        });
        debugRawPrinted++;
      }

      // --------------------------------------------
      // FILTER #1 — U.S. detection
      // --------------------------------------------
      const name = (actionGeoFullName || "").toLowerCase();

      const isUS =
        actionGeoCountry === "US" ||
        name.includes("united states") ||
        name.includes("usa") ||
        name.match(
          /\b(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV)\b/i
        );

      if (!isUS) continue;

      // --------------------------------------------
      // FILTER #2 — Geometry required
      // --------------------------------------------
      if (isNaN(actionGeoLat) || isNaN(actionGeoLon)) continue;

      // --------------------------------------------
      // FILTER #3 — Negativity (relaxed but meaningful)
      // --------------------------------------------
      // Debug stage: accept moderately negative or tense coverage.
      if (!(goldstein <= -1 || avgTone <= 0)) continue;

      countMatched++;

      // --------------------------------------------
      // DEBUG MATCH
      // --------------------------------------------
      console.log("GDELT MATCH:", {
        loc: actionGeoFullName,
        goldstein,
        avgTone,
        url,
      });

      // Require usable URL
      if (!url) continue;

      const publishedAt = parseGdeltDate(sqlDate);
      const domain = getDomain(url);

      const doc = {
        type: "news",
        source: "GDELT",
        domain,
        url,
        title: `${actionGeoFullName || "GDELT Event"} (${eventCode})`,
        description: `GDELT-coded event at ${actionGeoFullName || "unknown location"}`,
        publishedAt,
        createdAt: new Date(),
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000), // 24h TTL

        geometry: {
          type: "Point",
          coordinates: [actionGeoLon, actionGeoLat],
        },
        geometryMethod: "gdelt-action-geo",

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

      await col.updateOne({ url }, { $set: doc }, { upsert: true });
      countSaved++;
    }

    console.log(
      `🌎 GDELT FINISHED: matched ${countMatched} US events, saved ${countSaved}.`
    );
  } catch (err) {
    if (err.response) {
      console.error("❌ GDELT HTTP error:", err.response.status, err.response.data);
    } else {
      console.error("❌ GDELT poll error:", err.message);
    }
  }
}
