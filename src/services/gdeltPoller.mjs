// src/services/gdeltPoller.mjs
// ------------------------------------------------------------
// FINAL WORKING GDELT POLLER — Disaster Help
// ------------------------------------------------------------
// - TLS / hostname mismatch fixed via custom HTTPS agent
// - Uses correct 61-column GDELT v2 Events schema
// - Picks ActionGeo → Actor1Geo → Actor2Geo as location
// - Broad hazard detection (storms, outages, quakes, etc.)
// - Optional US-only filter (default OFF so you actually see data)
// - Bulk upserts + TTL cleanup
// ------------------------------------------------------------

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import https from "https";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

// Relaxed HTTPS agent for GDELT (broken hostname/cert combo)
const httpsAgent = new https.Agent({
  rejectUnauthorized: false,
});

// GDELT Events 2.0 schema is 61 columns (0..60):
// ... ActionGeo_Lat=56, ActionGeo_Long=57, DATEADDED=59, SOURCEURL=60
const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTCODE: 26,
  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1_LAT: 40,
  ACTOR1_LON: 41,

  ACTOR2_LAT: 48,
  ACTOR2_LON: 49,

  ACTION_LAT: 56,
  ACTION_LON: 57,

  ACTION_PLACE: 52,
  ACTION_COUNTRY: 53,

  DATEADDED: 59,
  SOURCEURL: 60,
});

const MIN_COLUMNS = 61;

// Domains we don't care about (celebrity / fluff)
const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "variety.com",
  "hollywoodreporter.com",
  "perezhilton",
  "eonline.com",
  "buzzfeed.com",
  "usmagazine.com",
  "entertainment",
];

// Root-code filter (OFF by default so we don't lose data)
const FILTER_ROOT_CODES =
  String(process.env.GDELT_FILTER_ROOT_CODES || "false").toLowerCase() === "true";

const ALLOWED_ROOT_CODES = new Set([
  "01", "02", "03", "04", "07", "08",
  "10", "11", "12",
  "14", "15", "16", "17", "18", "19", "20",
]);

// US-only filter (OFF by default so you actually see something)
const US_ONLY =
  String(process.env.GDELT_US_ONLY ?? "false").toLowerCase() === "true";

// Country codes for US + territories (FIPS + ISO)
const US_COUNTRY_CODES = new Set([
  "US", "USA",
  "AS", "GU", "MP", "PR", "VI", // ISO
  "AQ", "GQ", "CQ", "RQ", "VQ", // FIPS
]);

// TTL for news docs (hours). Default 24.
const TTL_HOURS = (() => {
  const n = Number.parseFloat(process.env.GDELT_TTL_HOURS);
  return Number.isFinite(n) && n > 0 ? n : 24;
})();
const TTL_MS = TTL_HOURS * 60 * 60 * 1000;

/* ------------------------------------------------------------------ */
/*  HELPERS                                                           */
/* ------------------------------------------------------------------ */

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

function looksLikeUSBounds(lon, lat) {
  const conus = lon >= -125 && lon <= -66 && lat >= 24 && lat <= 50;
  const ak = lon >= -170 && lon <= -130 && lat >= 50 && lat <= 72;
  const hi = lon >= -161 && lon <= -154 && lat >= 18 && lat <= 23;
  const pr = lon >= -67.5 && lon <= -65 && lat >= 17 && lat <= 19;
  return conus || ak || hi || pr;
}

function shouldKeepUS(placeCountry, lon, lat) {
  if (!US_ONLY) return true;
  const cc = String(placeCountry || "").trim().toUpperCase();
  if (cc && US_COUNTRY_CODES.has(cc)) return true;
  return looksLikeUSBounds(lon, lat);
}

function parseSqlDate(sqlDate) {
  const s = String(sqlDate || "").trim();
  if (!/^\d{8}$/.test(s)) return null;
  return new Date(`${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6)}T00:00:00Z`);
}

function parseDateAdded(dateAdded) {
  const s = String(dateAdded || "").trim();
  const m = s.match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
  if (!m) return null;
  const [_, Y, Mo, D, h, mi, se] = m;
  return new Date(Date.UTC(+Y, +Mo - 1, +D, +h, +mi, +se));
}

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isBlockedDomain(domain) {
  if (!domain) return true;
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => domain.includes(b));
}

/* ------------------------------------------------------------------ */
/*  COORD PICKER                                                      */
/* ------------------------------------------------------------------ */

function pickCoords(cols) {
  const aLat = parseFloat(cols[IDX.ACTION_LAT]);
  const aLon = parseFloat(cols[IDX.ACTION_LON]);
  if (isValidLonLat(aLon, aLat)) {
    return { lat: aLat, lon: aLon, method: "gdelt-action-geo" };
  }

  const a1Lat = parseFloat(cols[IDX.ACTOR1_LAT]);
  const a1Lon = parseFloat(cols[IDX.ACTOR1_LON]);
  if (isValidLonLat(a1Lon, a1Lat)) {
    return { lat: a1Lat, lon: a1Lon, method: "gdelt-actor1-geo" };
  }

  const a2Lat = parseFloat(cols[IDX.ACTOR2_LAT]);
  const a2Lon = parseFloat(cols[IDX.ACTOR2_LON]);
  if (isValidLonLat(a2Lon, a2Lat)) {
    return { lat: a2Lat, lon: a2Lon, method: "gdelt-actor2-geo" };
  }

  return null;
}

/* ------------------------------------------------------------------ */
/*  HAZARD DETECTION                                                  */
/* ------------------------------------------------------------------ */

function hazardHaystack(parts) {
  const raw = parts.filter(Boolean).join(" ");
  return raw.replace(/[_/\\.:;,-]+/g, " ").toLowerCase();
}

const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },
  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusts?|gusty)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe storm|damaging wind|hail)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|snow squall|whiteout|freezing rain|ice storm)\b/i },
  { label: "Flood", re: /\b(flood|flash flood|flooding|inundation|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },
  { label: "Power outage", re: /\b(power(?:\s|-)?outage|blackout|without power|no power|loss of power|downed (?:power )?lines?)\b/i },
  { label: "Hazmat / Explosion", re: /\b(hazmat|chemical spill|toxic leak|gas leak|industrial accident|explosion)\b/i },
];

function detectHazard(text) {
  if (!text) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  const MAX_SAVE = Number.isFinite(Number(process.env.GDELT_MAX_SAVE))
    ? Number(process.env.GDELT_MAX_SAVE)
    : 300;

  const BATCH_SIZE = Number.isFinite(Number(process.env.GDELT_BATCH_SIZE))
    ? Math.max(50, Number(process.env.GDELT_BATCH_SIZE))
    : 200;

  const now = new Date();
  let saved = 0;
  let debug = 0;

  try {
    // 1) Download lastupdate.txt (TLS bypass)
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      httpsAgent,
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const line = String(lastFile)
      .split(/\r?\n/)
      .find((l) => l.endsWith(".export.CSV.zip"));

    if (!line) {
      console.warn("⚠️ No GDELT export line found");
      return;
    }

    let zipUrl = line.trim().split(/\s+/).pop() || "";
    // Normalize to https
    zipUrl = zipUrl.replace(/^http:\/\//i, "https://");

    console.log("⬇️ Downloading GDELT ZIP:", zipUrl);

    // 2) Download ZIP stream (TLS bypass)
    const zipResp = await axios.get(zipUrl, {
      httpsAgent,
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    // 3) Extract CSV
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
      console.warn("⚠️ GDELT ZIP contained no CSV");
      return;
    }

    console.log("📄 Parsing GDELT CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    // TTL cleanup for GDELT docs
    try {
      await col.deleteMany({
        source: "GDELT",
        expires: { $lte: now },
      });
    } catch (e) {
      console.warn("TTL cleanup warn (GDELT):", e.message);
    }

    let ops = [];

    const flush = async () => {
      if (!ops.length) return;
      try {
        await col.bulkWrite(ops, { ordered: false });
      } catch (e) {
        console.warn("⚠️ GDELT bulkWrite warning:", e.message);
      }
      ops = [];
    };

    for await (const row of rl) {
      if (!row || !row.trim()) continue;

      const cols = row.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (!domain || isBlockedDomain(domain)) continue;

      if (FILTER_ROOT_CODES) {
        const rootCode = String(cols[IDX.EVENTROOTCODE] || "").trim();
        if (rootCode && !ALLOWED_ROOT_CODES.has(rootCode)) continue;
      }

      const coords = pickCoords(cols);
      if (!coords) continue;

      const place = cols[IDX.ACTION_PLACE] || "";
      const placeCountry = cols[IDX.ACTION_COUNTRY] || "";

      if (!shouldKeepUS(placeCountry, coords.lon, coords.lat)) continue;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";

      const haystack = hazardHaystack([
        actor1,
        actor2,
        place,
        placeCountry,
        url,
        domain,
      ]);

      const hazard = detectHazard(haystack);
      if (!hazard) continue;

      const sqlDate = cols[IDX.SQLDATE];
      const dateAdded = cols[IDX.DATEADDED];
      const publishedAt =
        parseDateAdded(dateAdded) || parseSqlDate(sqlDate) || now;

      const expires = new Date(publishedAt.getTime() + TTL_MS);

      if (debug < 10) {
        console.log("GDELT KEEP:", {
          hazard,
          place: place || "Unknown",
          domain,
          coords,
        });
        debug++;
      }

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel: hazard,
        url,
        domain,

        title: `${hazard} near ${place || "Unknown location"}`,
        description: `${hazard} reported near ${place || "Unknown location"}.`,

        publishedAt,
        updatedAt: now,
        expires,
        geometry: {
          type: "Point",
          coordinates: [coords.lon, coords.lat],
        },
        geometryMethod: coords.method,
        lat: coords.lat,
        lon: coords.lon,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate,
          dateAdded,
          rootCode: cols[IDX.EVENTROOTCODE],
          eventCode: cols[IDX.EVENTCODE],
          goldstein: Number.parseFloat(cols[IDX.GOLDSTEIN]),
          avgTone: Number.parseFloat(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          place,
          placeCountry,
        },
      };

      ops.push({
        updateOne: {
          filter: { url },
          update: {
            $set: doc,
            $setOnInsert: { createdAt: now },
          },
          upsert: true,
        },
      });

      saved++;
      if (ops.length >= BATCH_SIZE) await flush();
      if (saved >= MAX_SAVE) break;
    }

    await flush();

    console.log(`🌍 GDELT DONE — saved ${saved} events`);
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
