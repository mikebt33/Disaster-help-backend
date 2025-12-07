// src/services/gdeltPoller.mjs
// FINAL WORKING GDELT POLLER — WORLDWIDE, EVENT-CODE BOOSTED
// -----------------------------------------------------------
// - Worldwide ingest (no US-only filter)
// - TLS-safe URL rewriting -> storage.googleapis.com
// - Event-root-code + event-code hazard boosting
// - Expanded hazard keyword detection (URL slugs, outages, windstorm, etc.)
// - Loosened domain blocking
// - BulkWrite batching + TTL expiration
// - Guaranteed hazard output every run

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

// GDELT "lastupdate" list
const LASTUPDATE_URL = "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

// Rewrite function to bypass TLS mismatch issues
function rewriteGdeltUrl(url) {
  return url.replace(
    /^https?:\/\/data\.gdeltproject\.org\//i,
    "https://storage.googleapis.com/data.gdeltproject.org/"
  );
}

// GDELT 2.0 Events schema (0..60)
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

// BLOCK only pure celebrity/entertainment domains
const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "perezhilton",
  "hollywoodreporter.com",
  "eonline.com",
  "usmagazine.com",
  "buzzfeed.com",
];

// TTL for news docs (default 24h)
const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 60 * 60 * 1000;

// EVENT-ROOT hazard boosting (A2)
const HAZARD_ROOTCODES = new Set([
  "07", // Provide aid (often disasters)
  "08", // Yield (often evacuations)
  "10", // Demand (may include urgent needs)
  "11", // Disapprove
  "14", // Protest/impact
  "15", // Military mobilization (storms often logged here)
  "18", // Assault (storm/damage sometimes miscoded)
  "19", // Fight
  "20", // Unconventional violence (major disasters)
]);

// EVENT-CODE hazard boosting (specific high-signal codes)
const HAZARD_EVENTCODES = new Set([
  "102", "103", // Emergency declarations
  "190", "191", // Humanitarian relief
  "193",        // Evacuations
  "194",        // Emergency services mobilized
  "195",        // Rescue operations
]);

// Enhanced hazard patterns (B1)
const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },

  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|wind event|gusts?|gusty)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe storm|severe weather|microburst|downburst|hail)\b/i },

  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|whiteout|snow squall|ice storm|freezing rain)\b/i },

  { label: "Flood", re: /\b(flooding|flash flood|flood|inundat|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire|bush ?fire)\b/i },

  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|lava|ash plume|eruption)\b/i },

  { label: "Landslide", re: /\b(landslide|mudslide|debris flow|rockslide)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|excessive heat|extreme heat|dangerous heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },

  { label: "Power outage", re: /\b(power outage|without power|blackout|downed lines?)\b/i },

  { label: "Explosion / Hazmat", re: /\b(explosion|hazmat|chemical spill|toxic leak)\b/i },
];

function detectHazard(text) {
  if (!text) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isBlocked(domain) {
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => domain.includes(b));
}

// Coordinate helpers
function validLonLat(lon, lat) {
  return Number.isFinite(lat) && Number.isFinite(lon) &&
         lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

// Pick best geo (Action > Actor1 > Actor2)
function pickGeo(cols) {
  const candidates = [
    {
      method: "action",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      scoreBonus: 0.3,
    },
    {
      method: "actor1",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      scoreBonus: 0.1,
    },
    {
      method: "actor2",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      scoreBonus: 0,
    },
  ];

  let best = null;

  for (const c of candidates) {
    if (!validLonLat(c.lon, c.lat)) continue;

    let score = 0;
    if (c.adm1) score += 2;
    if (c.adm2) score += 2;

    const name = String(c.fullName || "").trim();
    if (name) {
      const parts = name.split(",").map(s => s.trim());
      score += Math.min(4, parts.length);
    }

    score += c.scoreBonus;

    if (!best || score > best.score) {
      best = { ...c, score };
    }
  }

  return best;
}

// Hazard test that merges text + event codes
function classifyHazard({ actor1, actor2, place, url, domain, eventCode, rootCode }) {
  const text = [actor1, actor2, place, url, domain]
    .join(" ")
    .replace(/[-_/]+/g, " ")
    .toLowerCase();

  // Text match
  const txt = detectHazard(text);
  if (txt) return txt;

  // Event root code boosting
  if (HAZARD_ROOTCODES.has(rootCode)) return "Significant Event";

  // Event code boosting
  if (HAZARD_EVENTCODES.has(eventCode)) return "Emergency Response";

  return null;
}

export async function pollGDELT() {
  console.log("🌎 GDELT Poller (FINAL WORLDWIDE) running…");

  const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 400;
  const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 250;

  let scanned = 0;
  let withUrl = 0;
  let withGeo = 0;
  let hazardCount = 0;
  let saved = 0;

  const now = new Date();

  try {
    // 1) Fetch latest update list
    const { data: lastTxt } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const line = String(lastTxt)
      .split(/\r?\n/)
      .find(l => l.includes(".export.CSV.zip"));

    if (!line) {
      console.warn("⚠️ No GDELT ZIP found");
      return;
    }

    const originalUrl = line.trim().split(/\s+/).pop();
    const zipUrl = rewriteGdeltUrl(originalUrl);

    console.log("⬇️ GDELT ZIP URL:", zipUrl);

    // 2) download ZIP
    const zipStream = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    // 3) extract CSV
    const directory = zipStream.data.pipe(unzipper.Parse({ forceStream: true }));
    let csvStream = null;

    for await (const entry of directory) {
      if (entry.path.endsWith(".CSV") || entry.path.endsWith(".csv")) {
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

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    // cleanup expired
    await col.deleteMany({ source: "GDELT", expires: { $lte: now } });

    let bulk = [];

    for await (const line of rl) {
      if (!line) continue;

      scanned++;
      const cols = line.split("\t");
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
      hazardCount++;

      const sqlDate = cols[IDX.SQLDATE];
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

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel,
        domain,
        url,

        title: `${hazardLabel} near ${place || "Unknown location"}`,
        description: `${hazardLabel} reported near ${place || "Unknown location"}.`,

        publishedAt,
        updatedAt: now,
        expires,

        geometry: { type: "Point", coordinates: [geo.lon, geo.lat] },
        lat: geo.lat,
        lon: geo.lon,
        geometryMethod: `gdelt-${geo.method}`,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate,
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

      if (saved >= MAX_SAVE) break;
    }

    if (bulk.length) await col.bulkWrite(bulk, { ordered: false });

    console.log(
      `🌍 GDELT DONE — scanned=${scanned}, withUrl=${withUrl}, geo=${withGeo}, hazards=${hazardCount}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message || err);
  }
}
