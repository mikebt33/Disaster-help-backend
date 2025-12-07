// src/services/gdeltPoller.mjs
// GDELT Poller — working, stable, TLS-bypass enabled
//
// Fixes:
// - Correct 61-col schema (SOURCEURL = column 60)
// - Smart geo selection (ActionGeo > Actor1 > Actor2)
// - Avoid generic "United States" center-point
// - Hazard detection upgraded (windstorm, outages, etc.)
// - TLS certificate bypass (Render-compatible)
// - US-only filter default ON (env override)
// - Batching + TTL + capped ingestion

// 🔥 Global TLS bypass for GDELT broken cert on Render
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import https from "https";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

// HTTPS agent used for GDELT requests
const httpsAgent = new https.Agent({ rejectUnauthorized: false });

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

// Optional root-code filter (usually OFF)
const FILTER_ROOT_CODES =
  String(process.env.GDELT_FILTER_ROOT_CODES || "false").toLowerCase() === "true";

const ALLOWED_ROOT_CODES = new Set([
  "01", "02", "03", "04", "07", "08",
  "10", "11", "12",
  "14", "15", "16", "17", "18", "19", "20",
]);

// Default ON: keep feed US-focused
const US_ONLY = String(process.env.GDELT_US_ONLY ?? "true").toLowerCase() !== "false";

// US + territories (FIPS + ISO variants)
const US_COUNTRY_CODES = new Set([
  "US", "USA",
  "AS", "GU", "MP", "PR", "VI", // ISO
  "AQ", "GQ", "CQ", "RQ", "VQ", // FIPS
]);

// TTL for news docs (hours)
const TTL_HOURS = Number.isFinite(Number.parseFloat(process.env.GDELT_TTL_HOURS))
  ? Number.parseFloat(process.env.GDELT_TTL_HOURS)
  : 24;
const TTL_MS = TTL_HOURS * 60 * 60 * 1000;

/* ------------------------------------------------------------------ */
/*  GEO HELPERS                                                       */
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

const COARSE_PLACE_RE = /^(united states|usa|canada|mexico)$/i;

function scoreGeoCandidate(c) {
  let score = 0;

  const t = parseInt(String(c.type ?? ""), 10);
  if (Number.isFinite(t)) score += Math.min(10, Math.max(0, t));

  if (c.adm1) score += 3;
  if (c.adm2) score += 2;

  const name = String(c.fullName || "").trim();
  if (name) {
    const parts = name.split(",").map((x) => x.trim()).filter(Boolean);
    score += Math.min(4, parts.length);
    if (parts.length === 1 && COARSE_PLACE_RE.test(parts[0])) score -= 8;
  } else {
    score -= 4;
  }

  const country = String(c.country || "").trim();
  if (country) score += 1;

  score += c.baseBonus || 0;
  return score;
}

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
      baseBonus: 0.3,
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
      baseBonus: 0.1,
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
      baseBonus: 0,
    },
  ];

  let best = null;
  for (const c of candidates) {
    if (!isValidLonLat(c.lon, c.lat)) continue;
    const sc = scoreGeoCandidate(c);
    if (!best || sc > best.score) best = { ...c, score: sc };
  }

  if (!best) return null;

  return {
    lon: best.lon,
    lat: best.lat,
    method: best.method,
    place: String(best.fullName || "").trim(),
    placeCountry: String(best.country || "").trim(),
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
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },

  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusts?|gusty)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe storm|damaging wind|hail)\b/i },

  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|snow squall|whiteout|ice storm)\b/i },

  { label: "Flood", re: /\b(flood|flash flood|flooding|inundation|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wildfire|forest fire|brush fire|grass fire)\b/i },

  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },

  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|excessive heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },

  { label: "Power outage", re: /\b(power(?:\s|-)?outage|blackout|without power|no power|loss of power|downed (?:power )?lines?)\b/i },

  { label: "Hazmat / Explosion", re: /\b(hazmat|chemical spill|toxic leak|gas leak|explosion)\b/i },
];

function detectHazardLabel(text = "") {
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

/* ------------------------------------------------------------------ */
/*  DOMAIN + BULK HELPERS                                             */
/* ------------------------------------------------------------------ */

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

  let scanned = 0;
  let withUrl = 0;
  let withCoords = 0;
  let keptUS = 0;
  let hazardMatched = 0;
  let saved = 0;

  const now = new Date();

  try {
    // 1) Determine latest events export file (with TLS bypass)
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      httpsAgent,
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const lines = String(lastFile).split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => /\d{14}\.export\.CSV\.zip$/.test(l));
    if (!zipLine) {
      console.warn("⚠️ No GDELT export.CSV.zip found in lastupdate.txt");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Download:", zipUrl);

    // 2) Download zip stream (with TLS bypass)
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
      console.warn("⚠️ ZIP contained no CSV");
      return;
    }

    console.log("📄 Parsing CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    // TTL cleanup just for GDELT docs
    try {
      await col.deleteMany({ source: "GDELT", type: "news", expires: { $lte: now } });
    } catch {
      /* ignore */
    }

    let debugPrinted = 0;
    let bulkOps = [];

    const flush = async () => {
      if (!bulkOps.length) return;
      try {
        await col.bulkWrite(bulkOps, { ordered: false });
      } catch (e) {
        console.warn("⚠️ GDELT bulkWrite warning:", e?.message || e);
      } finally {
        bulkOps = [];
      }
    };

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      scanned++;

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;
      withUrl++;

      const domain = getDomain(url);
      if (!domain || isBlockedDomain(domain)) continue;

      const rootCode = String(cols[IDX.EVENTROOTCODE] || "").trim();
      if (FILTER_ROOT_CODES && rootCode && !ALLOWED_ROOT_CODES.has(rootCode)) continue;

      const coords = pickBestCoords(cols);
      if (!coords) continue;
      withCoords++;

      if (!shouldKeepUS(coords.placeCountry, coords.lon, coords.lat)) continue;
      keptUS++;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place =
        coords.place ||
        cols[IDX.ACTIONGEO_FULLNAME] ||
        cols[IDX.ACTOR1GEO_FULLNAME] ||
        cols[IDX.ACTOR2GEO_FULLNAME] ||
        "Unknown location";

      const haystack = hazardHaystack([
        actor1,
        actor2,
        place,
        coords.placeCountry,
        url,
        domain,
        rootCode,
      ]);
      const hazardLabel = detectHazardLabel(haystack);
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
          placeCountry: coords.placeCountry,
          lon: coords.lon,
          lat: coords.lat,
          method: coords.method,
          rootCode,
          domain,
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

        geometry: { type: "Point", coordinates: [coords.lon, coords.lat] },
        location: { type: "Point", coordinates: [coords.lon, coords.lat] },
        geometryMethod: coords.method,
        lat: coords.lat,
        lon: coords.lon,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate,
          dateAdded,
          rootCode,
          eventCode: cols[IDX.EVENTCODE],
          goldstein: Number.parseFloat(cols[IDX.GOLDSTEIN]),
          avgTone: Number.parseFloat(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          place,
          placeCountry: coords.placeCountry,
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
        console.log(`🧯 Reached GDELT_MAX_SAVE=${MAX_SAVE}, stopping early.`);
        break;
      }
    }

    await flush();

    console.log(
      `🌍 GDELT DONE — scanned=${scanned}, urlOK=${withUrl}, geoOK=${withCoords}, USkept=${keptUS}, hazard=${hazardMatched}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  }
}
