// src/services/gdeltPoller.mjs
// ------------------------------------------------------------
// GDELT Poller — Upgraded with URL rewriting + valid TLS
// ------------------------------------------------------------
// - Uses canonical GCS URLs so TLS certificates are valid
// - Streams the latest GDELT v2 Events export
// - Correct 61-column schema (SOURCEURL = col 60)
// - Picks ActionGeo → Actor1Geo → Actor2Geo as location
// - Broad hazard detection (storms, floods, wildfires, outages, etc.)
// - Optional US-only filter (default: TRUE for your use case)
// - Bulk upserts into social_signals, with TTL-based expiration
// ------------------------------------------------------------

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

// Use canonical GCS URL for lastupdate with valid cert
// Docs show this form: https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt
const LASTUPDATE_URL =
  process.env.GDELT_LASTUPDATE_URL ||
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

// GDELT Events 2.0 schema indices (0..60)
const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTCODE: 26,
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

const MIN_COLUMNS = 61;

// domains we don't want (pure celeb/entertainment)
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

// Toggle root-code filter (off by default)
const FILTER_ROOT_CODES =
  String(process.env.GDELT_FILTER_ROOT_CODES || "false").toLowerCase() === "true";

const ALLOWED_ROOT_CODES = new Set([
  "01", "02", "03", "04", "07", "08",
  "10", "11", "12",
  "14", "15", "16", "17", "18", "19", "20",
]);

// US-only filter (on by default for your map; set GDELT_US_ONLY=false to see global)
const US_ONLY =
  String(process.env.GDELT_US_ONLY ?? "true").toLowerCase() === "true";

const US_COUNTRY_CODES = new Set([
  "US", "USA",
  "AS", "GU", "MP", "PR", "VI", // ISO territories
  "AQ", "GQ", "CQ", "RQ", "VQ", // FIPS territories
]);

// TTL hours for GDELT news
const TTL_HOURS = (() => {
  const n = Number.parseFloat(process.env.GDELT_TTL_HOURS);
  return Number.isFinite(n) && n > 0 ? n : 24;
})();
const TTL_MS = TTL_HOURS * 60 * 60 * 1000;

/* ------------------------------------------------------------------ */
/*  HELPERS                                                           */
/* ------------------------------------------------------------------ */

function rewriteToGcsUrl(url) {
  // GDELT inside lastupdate uses http://data.gdeltproject.org/... by default.
  // Replace with canonical GCS URL which has a valid certificate.
  if (!url) return url;
  return url.replace(
    /^https?:\/\/data\.gdeltproject\.org\//i,
    "https://storage.googleapis.com/data.gdeltproject.org/"
  );
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

/* ------------------------------------------------------------------ */
/*  GEO SELECTION                                                     */
/* ------------------------------------------------------------------ */

function pickBestCoords(cols) {
  const candidates = [
    {
      method: "gdelt-action-geo",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      type: cols[IDX.ACTIONGEO_TYPE],
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      bonus: 0.3,
    },
    {
      method: "gdelt-actor1-geo",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      type: cols[IDX.ACTOR1GEO_TYPE],
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      bonus: 0.1,
    },
    {
      method: "gdelt-actor2-geo",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      type: cols[IDX.ACTOR2GEO_TYPE],
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      bonus: 0,
    },
  ];

  let best = null;

  for (const c of candidates) {
    if (!isValidLonLat(c.lon, c.lat)) continue;

    let score = c.bonus;
    const t = Number.parseInt(c.type, 10);
    if (Number.isFinite(t)) score += Math.min(10, Math.max(0, t));

    if (c.adm1) score += 2;
    if (c.adm2) score += 1;

    const name = String(c.fullName || "").trim();
    if (name) {
      const parts = name.split(",").map((p) => p.trim()).filter(Boolean);
      score += Math.min(4, parts.length);
    }

    if (!best || score > best.score) best = { ...c, score };
  }

  if (!best) return null;

  return {
    lon: best.lon,
    lat: best.lat,
    method: best.method,
    place: String(best.fullName || "").trim(),
    country: String(best.country || "").trim(),
  };
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
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },
  { label: "High wind", re: /\b(high winds?|damaging winds?|windstorm|gusts?|gusty)\b/i },
  { label: "Severe storm", re: /\b(severe storm|thunderstorm|damaging wind|hail)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|snow squall|ice storm|whiteout)\b/i },
  { label: "Flooding", re: /\b(flood|flash flood|flooding|inundation|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },
  { label: "Power outage", re: /\b(power(?:\s|-)?outage|blackout|without power|no power|loss of power|downed (?:power )?lines?)\b/i },
  { label: "Hazmat / Explosion", re: /\b(hazmat|chemical spill|toxic leak|gas leak|industrial accident|explosion)\b/i },
];

function detectHazardLabel(text = "") {
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

export async function pollGDELT() {
  console.log("🌎 GDELT Poller (URL-rewrite, TLS-ok) running…");

  const MAX_SAVE = Number.isFinite(Number(process.env.GDELT_MAX_SAVE))
    ? Number(process.env.GDELT_MAX_SAVE)
    : 300;

  const BATCH_SIZE = Number.isFinite(Number(process.env.GDELT_BATCH_SIZE))
    ? Math.max(50, Number(process.env.GDELT_BATCH_SIZE))
    : 200;

  let saved = 0;
  let scanned = 0;
  let hazardMatched = 0;

  const now = new Date();

  try {
    // STEP 1: Fetch lastupdate.txt from GCS (valid cert)
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const lines = String(lastFile).split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => /\d{14}\.export\.CSV\.zip$/.test(l));

    if (!zipLine) {
      console.warn("⚠️ GDELT: No export CSV reference found in lastupdate.txt");
      return;
    }

    let rawZipUrl = zipLine.trim().split(/\s+/).pop();
    const zipUrl = rewriteToGcsUrl(rawZipUrl);
    console.log("⬇️ Downloading GDELT ZIP:", zipUrl);

    // STEP 2: Download ZIP
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    // STEP 3: Extract CSV
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
      console.warn("⚠️ GDELT: ZIP contained no CSV");
      return;
    }

    console.log("📄 Parsing GDELT Events CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    // TTL cleanup for previous GDELT docs
    try {
      await col.deleteMany({
        source: "GDELT",
        expires: { $lte: now },
      });
    } catch (e) {
      console.warn("TTL cleanup warning (GDELT):", e.message);
    }

    let bulkOps = [];
    let debugPrinted = 0;

    const flush = async () => {
      if (!bulkOps.length) return;
      try {
        await col.bulkWrite(bulkOps, { ordered: false });
      } catch (e) {
        console.warn("⚠️ GDELT bulkWrite warning:", e.message);
      }
      bulkOps = [];
    };

    for await (const line of rl) {
      if (!line.trim()) continue;
      scanned++;

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (!domain || isBlockedDomain(domain)) continue;

      if (FILTER_ROOT_CODES) {
        const rootCode = String(cols[IDX.EVENTROOTCODE] || "").trim();
        if (rootCode && !ALLOWED_ROOT_CODES.has(rootCode)) continue;
      }

      const coords = pickBestCoords(cols);
      if (!coords) continue;

      if (!shouldKeepUS(coords.country, coords.lon, coords.lat)) continue;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place =
        coords.place || cols[IDX.ACTIONGEO_FULLNAME] || "Unknown location";

      const hay = hazardHaystack([
        actor1,
        actor2,
        place,
        coords.country,
        url,
        domain,
      ]);

      const hazardLabel = detectHazardLabel(hay);
      if (!hazardLabel) continue;
      hazardMatched++;

      const sqlDate = cols[IDX.SQLDATE];
      const dateAdded = cols[IDX.DATEADDED];

      const publishedAt =
        parseDateAdded(dateAdded) || parseSqlDate(sqlDate) || now;

      const expires = new Date(publishedAt.getTime() + TTL_MS);

      if (debugPrinted < 10) {
        console.log("GDELT KEEP:", {
          hazardLabel,
          place,
          domain,
          coords,
        });
        debugPrinted++;
      }

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel,
        domain,
        url,

        title: `${hazardLabel} near ${place}`,
        description: `${hazardLabel} reported near ${place}.`,

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
          placeCountry: coords.country,
        },
      };

      bulkOps.push({
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
      if (bulkOps.length >= BATCH_SIZE) await flush();
      if (saved >= MAX_SAVE) {
        console.log(
          `🧯 Reached GDELT_MAX_SAVE=${MAX_SAVE}, stopping ingestion early.`
        );
        break;
      }
    }

    await flush();

    console.log(
      `🌍 GDELT DONE — scanned=${scanned}, hazardMatched=${hazardMatched}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
