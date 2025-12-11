// src/services/gdeltPoller.mjs
// GDELT Poller — Worldwide, hazard-max, TLS-safe, event-code boosted.
// -------------------------------------------------------------------
// - Worldwide ingest (no US-only restriction)
// - TLS-safe URL rewriting -> storage.googleapis.com
// - Event-root-code + event-code hazard boosting
// - Expanded hazard keyword detection (URL slugs, outages, windstorm, etc.)
// - Damage/impact fallback classification
// - Loosened domain blocking (celebrity-only)
// - BulkWrite batching + TTL expiration
// - Micro-jitter to reduce stacking without breaking map clustering

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

// MUST load lastupdate.txt from storage.googleapis.com (TLS-safe)
const LASTUPDATE_URL =
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT =
  process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

// rewrite ZIP URLs from data.gdeltproject.org → storage.googleapis.com
function rewriteGdeltUrl(url) {
  // handle both http/https and ensure gdeltv2 path is preserved
  return url
    .replace(
      /^https?:\/\/data\.gdeltproject\.org\//i,
      "https://storage.googleapis.com/data.gdeltproject.org/"
    );
}

// ---------------------------------------------------------------------------
// GDELT Schema (61 columns)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Blocking / TTL / batch config
// ---------------------------------------------------------------------------

const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "perezhilton",
  "hollywoodreporter.com",
  "eonline.com",
  "usmagazine.com",
  "buzzfeed.com",
];

const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 3600 * 1000;

const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 400;
const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 250;

// helper: extract domain from URL
function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

// helper: check if domain should be blocked
function isBlocked(domain) {
  if (!domain) return true;
  const d = domain.toLowerCase();
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => d.includes(b));
}

// ---------------------------------------------------------------------------
// Hazard boosting — roots + event codes + text patterns
// ---------------------------------------------------------------------------

const HAZARD_ROOTCODES = new Set([
  "07",
  "08",
  "10",
  "11",
  "14",
  "15",
  "18",
  "19",
  "20",
]);

const HAZARD_EVENTCODES = new Set([
  "102",
  "103",
  "190",
  "191",
  "193",
  "194",
  "195",
]);

const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },
  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusty)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe storm|hail|microburst|downburst)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|whiteout|ice storm)\b/i },
  { label: "Flood", re: /\b(flooding|flash flood|inundat|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|lava|eruption|ash plume)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow|rockslide)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|excessive heat|dangerous heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },
  { label: "Power outage", re: /\b(power outage|blackout|downed lines?)\b/i },
  { label: "Explosion / Hazmat", re: /\b(explosion|hazmat|chemical spill|toxic leak)\b/i },
];

const DAMAGE_PATTERN =
  /\b(collaps(ed|e)|destroyed|damaged|washed (away|out)|swept away|missing|injured|killed|fatalities|dead|displaced|evacuate|rescued|stranded|trapped)\b/i;

const GENERIC_IMPACT_LABEL = "Significant Impact Event";

function detectHazardFromText(t) {
  if (!t) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(t)) return label;
  }
  return null;
}

function classifyHazard({
  actor1,
  actor2,
  place,
  url,
  domain,
  eventCode,
  rootCode,
}) {
  const text = [actor1, actor2, place, url, domain]
    .join(" ")
    .replace(/[-_/]+/g, " ")
    .toLowerCase();

  const txt = detectHazardFromText(text);
  if (txt) return txt;

  if (DAMAGE_PATTERN.test(text)) return GENERIC_IMPACT_LABEL;

  if (HAZARD_ROOTCODES.has(rootCode)) return "Significant Event";
  if (HAZARD_EVENTCODES.has(eventCode)) return "Emergency Response";

  return null;
}

// ---------------------------------------------------------------------------
// GEO helpers
// ---------------------------------------------------------------------------

function validLonLat(lon, lat) {
  return (
    Number.isFinite(lon) &&
    Number.isFinite(lat) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

function pickGeo(cols) {
  const c = [
    {
      method: "action",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      bonus: 0.3,
    },
    {
      method: "actor1",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      bonus: 0.15,
    },
    {
      method: "actor2",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      bonus: 0.05,
    },
  ];

  let best = null;
  for (const x of c) {
    if (!validLonLat(x.lon, x.lat)) continue;

    let score = x.bonus;
    if (x.adm1) score += 1.5;
    if (x.adm2) score += 1.0;
    if (x.fullName) score += Math.min(4, x.fullName.split(",").length);

    if (!best || score > best.score) best = { ...x, score };
  }

  return best;
}

function microJitter(lon, lat, seed, mag = 0.12) {
  let h = 0x811c9dc5;
  const s = String(seed || "");
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  const rand = () => {
    h += 0x6d2b79f5;
    let t = Math.imul(h ^ (h >>> 15), 1 | h);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const jLon = lon + (rand() - 0.5) * mag;
  const jLat = lat + (rand() - 0.5) * mag;
  return validLonLat(jLon, jLat) ? [jLon, jLat] : [lon, lat];
}

// ---------------------------------------------------------------------------
// MAIN POLLER
// ---------------------------------------------------------------------------

export async function pollGDELT() {
  console.log("🌎 GDELT Poller (HAZARD-MAX WORLDWIDE) running…");

  let scanned = 0;
  let withUrl = 0;
  let withGeo = 0;
  let hazards = 0;
  let saved = 0;

  const now = new Date();

  try {
    // -------------------------------------------------------------
    // 1) Fetch lastupdate.txt from GOOGLE CLOUD (TLS SAFE)
    // -------------------------------------------------------------
    const { data: lastTxt } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const line = String(lastTxt)
      .split(/\r?\n/)
      .find((l) => l.includes(".export.CSV.zip"));

    if (!line) {
      console.warn("⚠️ No GDELT export ZIP found");
      return;
    }

    const originalUrl = line.trim().split(/\s+/).pop();
    const zipUrl = rewriteGdeltUrl(originalUrl);

    console.log("⬇️ GDELT ZIP URL:", zipUrl);

    // -------------------------------------------------------------
    // 2) Download ZIP (TLS-safe)
    // -------------------------------------------------------------
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    // -------------------------------------------------------------
    // 3) Extract CSV
    // -------------------------------------------------------------
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
      console.warn("⚠️ ZIP had no CSV");
      return;
    }

    console.log("📄 Parsing GDELT Events CSV…");

    // -------------------------------------------------------------
    // 4) Parse + classify + save
    // -------------------------------------------------------------
    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    // TTL cleanup for GDELT docs only
    await col.deleteMany({ source: "GDELT", expires: { $lte: now } });

    let bulk = [];

    for await (const l of rl) {
      if (!l) continue;

      scanned++;
      const cols = l.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;
      withUrl++;

      const domain = getDomain(url);
      if (!domain || isBlocked(domain)) continue;

      const geo = pickGeo(cols);
      if (!geo) continue;
      withGeo++;

      const eventCode = String(cols[IDX.EVENTCODE] || "").trim();
      const rootCode = String(cols[IDX.EVENTROOTCODE] || "").trim();

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place = geo.fullName || "";

      const hazardLabel = classifyHazard({
        actor1,
        actor2,
        place,
        url,
        domain,
        eventCode,
        rootCode,
      });

      if (!hazardLabel) continue;
      hazards++;

      // Timestamp
      const dateAdded = cols[IDX.DATEADDED];
      let publishedAt = now;

      if (/^\d{14}$/.test(dateAdded)) {
        const Y = dateAdded.slice(0, 4);
        const M = dateAdded.slice(4, 6);
        const D = dateAdded.slice(6, 8);
        const h = dateAdded.slice(8, 10);
        const m = dateAdded.slice(10, 12);
        const s = dateAdded.slice(12, 14);
        publishedAt = new Date(`${Y}-${M}-${D}T${h}:${m}:${s}Z`);
      }

      const expires = new Date(publishedAt.getTime() + TTL_MS);

      // Micro-jitter
      const [jLon, jLat] = microJitter(
        geo.lon,
        geo.lat,
        `${cols[IDX.GLOBALEVENTID]}|${url}`,
        0.12
      );

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel,
        domain,
        url,

        title: `${hazardLabel} near ${place || "Unknown location"}`,
        description: `${hazardLabel} reported near ${
          place || "Unknown location"
        }.`,

        publishedAt,
        updatedAt: now,
        expires,

        geometry: { type: "Point", coordinates: [jLon, jLat] },
        lat: jLat,
        lon: jLon,
        geometryMethod: `gdelt-${geo.method}`,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate: cols[IDX.SQLDATE],
          dateAdded,
          eventCode,
          rootCode,
          goldstein: Number(cols[IDX.GOLDSTEIN]),
          avgTone: Number(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          place,
          country: geo.country,
        },
      };

      bulk.push({
        updateOne: {
          filter: { url },
          update: { $set: doc, $setOnInsert: { createdAt: now } },
          upsert: true,
        },
      });

      saved++;

      if (bulk.length >= BATCH_SIZE) {
        await col.bulkWrite(bulk, { ordered: false });
        bulk = [];
      }

      if (saved >= MAX_SAVE) {
        console.log(`🧯 Reached MAX_SAVE=${MAX_SAVE}, stopping early.`);
        break;
      }
    }

    if (bulk.length) await col.bulkWrite(bulk, { ordered: false });

    console.log(
      `🌍 GDELT DONE — scanned=${scanned}, urls=${withUrl}, geo=${withGeo}, hazards=${hazards}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  }
}
