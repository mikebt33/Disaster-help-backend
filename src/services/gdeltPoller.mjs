// src/services/gdeltPoller.mjs
// GDELT Poller — A2 Disaster/Weather-Only Mode
// Produces clean, civilian-friendly quicksheet text.

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

// ------------------ DISASTER KEYWORDS ------------------
const DISASTER_KEYWORDS = [
  "storm", "thunderstorm", "severe weather", "winter", "snow", "blizzard",
  "ice", "freezing", "cold wave", "wind", "damaging wind",
  "hurricane", "cyclone", "typhoon", "tropical storm",
  "wildfire", "fire", "bushfire", "smoke", "air quality",
  "flood", "flash flood", "flooding",
  "landslide", "mudslide",
  "earthquake", "aftershock", "seismic",
  "eruption", "volcano",
  "drought", "heat wave", "extreme heat",
  "outage", "blackout", "power outage",
  "explosion", "hazmat", "chemical", "toxic"
];

// ------------------ HAZARD CLASSIFIER ------------------
function classifyHazard(text = "") {
  const t = text.toLowerCase();

  if (t.match(/wildfire|fire|bushfire|smoke/)) return "Wildfire activity";
  if (t.match(/storm|thunderstorm|severe weather|wind/)) return "Severe storm activity";
  if (t.match(/snow|winter|blizzard|ice|freezing/)) return "Winter storm conditions";
  if (t.match(/flood|flash flood|flooding/)) return "Flooding impacts";
  if (t.match(/earthquake|aftershock|seismic/)) return "Earthquake activity";
  if (t.match(/landslide|mudslide/)) return "Landslide conditions";
  if (t.match(/drought|heat wave|extreme heat/)) return "Extreme heat conditions";
  if (t.match(/outage|blackout/)) return "Power outage";
  if (t.match(/hazmat|chemical|toxic|explosion/)) return "Hazardous material incident";

  return "Hazard conditions";
}

// ------------------ MAPPINGS ------------------
const BLOCKED_DOMAINS = [
  "tmz.com", "people.com", "variety.com", "hollywoodreporter.com", "geonews",
  "perezhilton", "eonline.com", "buzzfeed.com", "usmagazine.com", "entertainment",
];

// Disaster root codes from GDELT taxonomy
const DISASTER_ROOTS = new Set(["07","08","14","15","16","17","18","19","20"]);

// Date parser
function parseGdeltDate(sqlDate) {
  if (!sqlDate || sqlDate.length !== 8) return new Date();
  return new Date(`${sqlDate.slice(0, 4)}-${sqlDate.slice(4, 6)}-${sqlDate.slice(6)}T00:00:00Z`);
}

// Domain extractor
function getDomain(url) {
  try { return new URL(url).hostname.replace(/^www\./, ""); }
  catch { return ""; }
}

// ------------------ MAIN POLLER ------------------
export async function pollGDELT() {
  console.log("🌎 GDELT Poller running (Disaster-Only A2)…");

  try {
    // --------- 1. Get latest events file ---------
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
    });

    const lines = lastFile.split(/\r?\n/).filter(Boolean);

    // Match correct EVENTS export ZIP
    const zipLine = lines.find((l) => l.match(/\/\d{14}\.export\.CSV\.zip$/));
    if (!zipLine) {
      console.warn("⚠️ No valid events file found.");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading:", zipUrl);

    // --------- 2. Download ZIP ---------
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
    });

    // --------- 3. Extract CSV ---------
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
      console.warn("⚠️ ZIP had no CSV.");
      return;
    }

    console.log("📄 Parsing CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let debugPrinted = 0;
    let matched = 0;
    let saved = 0;

    // ------------------ MAIN CSV LOOP ------------------
    for await (const line of rl) {
      if (!line.trim()) continue;
      const cols = line.split("\t");

      if (cols.length < 61) continue; // malformed or wrong file

      // Column mapping for GDELT Events V2.0
      const sqlDate = cols[1];
      const eventRootCode = cols[28];
      const goldstein = parseFloat(cols[30]);
      const avgTone = parseFloat(cols[34]);
      const fullName = cols[52];
      const country = cols[53];
      const lat = parseFloat(cols[56]);
      const lon = parseFloat(cols[57]);
      const url = cols[60];

      // ---- Debug first 15 raw rows ----
      if (debugPrinted < 15) {
        console.log("RAW:", { sqlDate, fullName, country, root: eventRootCode, goldstein, avgTone, url, lat, lon });
        debugPrinted++;
      }

      // ---------- FILTER 1: Must be US ----------
      const name = (fullName || "").toLowerCase();
      const isUS =
        country === "US" ||
        name.includes("united states") ||
        name.includes("usa") ||
        name.match(/\b(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV)\b/i);

      if (!isUS) continue;

      // ---------- FILTER 2: Must have coordinates ----------
      if (isNaN(lat) || isNaN(lon)) continue;

      // ---------- FILTER 3: Must have URL ----------
      if (!url) continue;

      const domain = getDomain(url);

      // ---------- FILTER 4: Block gossip ----------
      if (BLOCKED_DOMAINS.some((b) => domain.includes(b))) continue;

      // ---------- FILTER 5: Must be disaster root code ----------
      if (!DISASTER_ROOTS.has(eventRootCode)) continue;

      // ---------- FILTER 6: Keyword match ----------
      const textLower = (fullName || "").toLowerCase();
      const keywordHit = DISASTER_KEYWORDS.some((kw) => textLower.includes(kw));
      if (!keywordHit) continue;

      // ---------- FILTER 7: Negativity threshold ----------
      if (!(goldstein <= -1 || avgTone <= 0)) continue;

      matched++;

      // ---------- CLASSIFY ----------
      const hazard = classifyHazard(fullName);
      const publishedAt = parseGdeltDate(sqlDate);

      // ---------- HUMAN-FRIENDLY DOCUMENT ----------
      const doc = {
        type: "news",
        source: "GDELT",
        domain,
        url,

        title: `${hazard} near ${fullName || "Unknown Location"}`,
        description: `${hazard} reported near ${fullName}.`,

        publishedAt,
        createdAt: new Date(),
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000),

        geometry: {
          type: "Point",
          coordinates: [lon, lat],
        },
        geometryMethod: "gdelt-action-geo",

        gdelt: {
          sqlDate,
          eventRootCode,
          goldstein,
          avgTone,
          fullName,
        },
      };

      await col.updateOne({ url }, { $set: doc }, { upsert: true });
      saved++;
    }

    console.log(`🌎 GDELT DONE — Matched ${matched} disaster events, saved ${saved}.`);
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
