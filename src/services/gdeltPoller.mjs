// src/services/gdeltPoller.mjs
// GDELT Poller — DEBUG BUILD
// Prints detailed skip reasons so we can identify why 100% of rows are filtered out.

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

const MIN_COLUMNS = 61;

// -------------------- Schema indices --------------------
const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1GEO_TYPE: 35,
  ACTOR1GEO_FULLNAME: 36,
  ACTOR1GEO_COUNTRY: 37,
  ACTOR1GEO_ADM1: 38,
  ACTOR1GEO_ADM2: 39,
  ACTOR1GEO_LAT: 40,
  ACTOR1GEO_LON: 41,

  ACTOR2GEO_TYPE: 43,
  ACTOR2GEO_FULLNAME: 44,
  ACTOR2GEO_COUNTRY: 45,
  ACTOR2GEO_ADM1: 46,
  ACTOR2GEO_ADM2: 47,
  ACTOR2GEO_LAT: 48,
  ACTOR2GEO_LON: 49,

  ACTIONGEO_TYPE: 51,
  ACTIONGEO_FULLNAME: 52,
  ACTIONGEO_COUNTRY: 53,
  ACTIONGEO_ADM1: 54,
  ACTIONGEO_ADM2: 55,
  ACTIONGEO_LAT: 56,
  ACTIONGEO_LON: 57,

  DATEADDED: 59,
  SOURCEURL: 60,
});

// -------------------- Debug settings --------------------
const DEBUG_LIMIT = 300;
let debugCount = 0;

function debugSkip(reason, row) {
  if (debugCount < DEBUG_LIMIT) {
    console.log("GDELT DEBUG: SKIP reason=" + reason, {
      url: row[IDX.SOURCEURL],
      place: row[IDX.ACTIONGEO_FULLNAME],
      actor1: row[IDX.ACTOR1NAME],
      actor2: row[IDX.ACTOR2NAME],
      lat: row[IDX.ACTIONGEO_LAT],
      lon: row[IDX.ACTIONGEO_LON],
    });
  }
  debugCount++;
}

// -------------------- Domain filtering --------------------
const BLOCKED = [
  "tmz.com", "people.com", "variety.com",
  "hollywoodreporter.com", "perezhilton",
  "eonline.com", "buzzfeed.com",
  "usmagazine.com", "entertainment",
];

// -------------------- Hazard detection --------------------
const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },
  { label: "High wind", re: /\b(high winds?|windstorm|gusts?)\b/i },
  { label: "Severe storm", re: /\b(severe storm|thunderstorm|hail)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|ice storm)\b/i },
  { label: "Flood", re: /\b(flood|flash flood|inundation)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|brush fire|forest fire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|extreme heat)\b/i },
  { label: "Power outage", re: /\b(outage|without power|blackout)\b/i },
];

function detectHazardLabel(text = "") {
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

// -------------------- Geo scoring --------------------
function isValidLonLat(lon, lat) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

function scoreCandidate(c) {
  let score = 0;

  const t = Number.parseInt(c.type, 10);
  if (Number.isFinite(t)) score += t;

  if (c.adm1) score += 2;
  if (c.adm2) score += 2;

  const name = String(c.fullName || "").trim();
  if (name) {
    const parts = name.split(",").map(s => s.trim()).filter(Boolean);
    score += Math.min(parts.length, 4);
  }

  score += c.baseBonus;
  return score;
}

function pickBestCoords(cols) {
  const candidates = [
    {
      method: "action",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      type: cols[IDX.ACTIONGEO_TYPE],
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      baseBonus: 0.25,
    },
    {
      method: "actor1",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      type: cols[IDX.ACTOR1GEO_TYPE],
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      baseBonus: 0.15,
    },
    {
      method: "actor2",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      type: cols[IDX.ACTOR2GEO_TYPE],
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      baseBonus: 0.05,
    },
  ];

  let best = null;

  for (const c of candidates) {
    if (!isValidLonLat(c.lon, c.lat)) continue;
    const s = scoreCandidate(c);
    if (!best || s > best.score) best = { ...c, score: s };
  }

  return best;
}

function hazardHaystack(parts) {
  return parts.filter(Boolean).join(" ").replace(/[_/\\.-]+/g, " ").toLowerCase();
}

// -------------------- MAIN POLLER --------------------
export async function pollGDELT() {
  console.log("🌎 GDELT DEBUG Poller running…");
  debugCount = 0;

  const now = new Date();
  const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 300;

  try {
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const lines = String(lastFile).split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => /\d{14}\.export\.CSV\.zip$/.test(l));

    if (!zipLine) {
      console.log("GDELT DEBUG: No export file listed");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading GDELT ZIP:", zipUrl);

    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    let csvStream = null;
    const directory = zipResp.data.pipe(unzipper.Parse({ forceStream: true }));

    for await (const entry of directory) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry;
        break;
      }
      entry.autodrain();
    }

    if (!csvStream) {
      console.log("GDELT DEBUG: ZIP has no CSV");
      return;
    }

    console.log("📄 Parsing GDELT CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let bulkOps = [];
    let saved = 0;

    for await (const line of rl) {
      if (!line.trim()) continue;

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) {
        debugSkip("too-few-columns", cols);
        continue;
      }

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//.test(url)) {
        debugSkip("invalid-url", cols);
        continue;
      }

      const domain = getDomain(url);
      if (!domain) {
        debugSkip("no-domain", cols);
        continue;
      }

      if (BLOCKED.some((b) => domain.includes(b))) {
        debugSkip("blocked-domain", cols);
        continue;
      }

      const coords = pickBestCoords(cols);
      if (!coords) {
        debugSkip("no-valid-coords", cols);
        continue;
      }

      const actor1 = cols[IDX.ACTOR1NAME];
      const actor2 = cols[IDX.ACTOR2NAME];
      const place = coords.fullName || cols[IDX.ACTIONGEO_FULLNAME];

      const haystack = hazardHaystack([actor1, actor2, place, url]);
      const hazard = detectHazardLabel(haystack);
      if (!hazard) {
        debugSkip("no-hazard-match", cols);
        continue;
      }

      // ========== KEEP ==========
      console.log("GDELT KEEP:", {
        hazard,
        place,
        domain,
        lat: coords.lat,
        lon: coords.lon,
        method: coords.method,
      });

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel: hazard,
        domain,
        url,
        title: `${hazard} near ${place}`,
        description: `${hazard} reported near ${place}.`,
        publishedAt: now,
        updatedAt: now,
        expires: new Date(now.getTime() + 24 * 3600 * 1000),
        geometry: { type: "Point", coordinates: [coords.lon, coords.lat] },
      };

      bulkOps.push({
        updateOne: {
          filter: { url },
          update: { $set: doc, $setOnInsert: { createdAt: now } },
          upsert: true,
        },
      });

      saved++;
      if (saved >= MAX_SAVE) break;
      if (bulkOps.length >= 100) {
        await col.bulkWrite(bulkOps, { ordered: false });
        bulkOps = [];
      }
    }

    if (bulkOps.length) await col.bulkWrite(bulkOps, { ordered: false });

    console.log(`🌍 GDELT DEBUG DONE — saved=${saved}, skipped=${debugCount}`);

  } catch (err) {
    console.error("❌ GDELT DEBUG ERROR:", err.message);
  }
}
