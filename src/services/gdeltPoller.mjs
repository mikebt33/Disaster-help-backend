// src/services/gdeltPoller.mjs
// GDELT Poller — Worldwide, maximum signals, TLS-safe.
// -------------------------------------------------------------------
// - Worldwide ingest (no US-only restriction)
// - TLS-safe URL rewriting -> storage.googleapis.com
// - Event-root-code + event-code hazard boosting
// - Expanded hazard keyword detection (URL slugs, outages, windstorm, etc.)
// - Damage/impact fallback classification
// - Loosened domain blocking (celebrity / entertainment only)
// - BulkWrite batching + TTL expiration
// - Micro-jitter to reduce stacking without breaking map clustering
// - Always tries hard to assign a hazard label so the map stays busy

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
  return url.replace(
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
  // celebrity / entertainment / gossip
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

const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 600; // keep the map lively
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
  if (!domain) return false; // ⬅️ DO NOT BLOCK EMPTY DOMAINS
  const d = domain.toLowerCase();
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => d.includes(b));
}

// ---------------------------------------------------------------------------
// Hazard boosting — roots + event codes + text patterns
// ---------------------------------------------------------------------------

// Expand root codes a bit for maximum signal (GDELT CAMEO roots)
const HAZARD_ROOTCODES = new Set([
  "07", // protest / violent, often used around disaster protests/riots
  "08",
  "10", // natural disasters, infrastructure
  "11",
  "14",
  "15",
  "18",
  "19",
  "20",
]);

const HAZARD_EVENTCODES = new Set([
  // emergency / disaster-related events
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
  {
    label: "Hurricane / Tropical",
    re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i,
  },
  {
    label: "High Wind",
    re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusty)\b/i,
  },
  {
    label: "Severe Storm",
    re: /\b(thunderstorm|severe storm|hail|microburst|downburst)\b/i,
  },
  {
    label: "Winter Storm",
    re: /\b(blizzard|winter storm|snowstorm|whiteout|ice storm)\b/i,
  },
  {
    label: "Flood",
    re: /\b(flood(ing)?|flash flood|inundat|storm surge|dam burst|levee)\b/i,
  },
  {
    label: "Wildfire",
    re: /\b(wild ?fire|forest fire|brush fire|grass fire|bushfire)\b/i,
  },
  {
    label: "Earthquake",
    re: /\b(earthquake|aftershock|seismic|richter)\b/i,
  },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|lava|eruption|ash plume)\b/i },
  {
    label: "Landslide",
    re: /\b(landslide|mudslide|debris flow|rockslide|slope failure)\b/i,
  },
  {
    label: "Extreme Heat",
    re: /\b(heat wave|heatwave|excessive heat|dangerous heat)\b/i,
  },
  { label: "Drought", re: /\b(drought|water shortage|dry spell)\b/i },
  {
    label: "Power Outage",
    re: /\b(power outage|blackout|power cut|grid failure|downed lines?)\b/i,
  },
  {
    label: "Explosion / Hazmat",
    re: /\b(explosion|blast|hazmat|chemical spill|toxic leak|gas leak)\b/i,
  },
  {
    label: "Storm / Weather",
    re: /\b(severe weather|storm damage|gale force|monsoon|typhoon)\b/i,
  },
];

const DAMAGE_PATTERN =
  /\b(collaps(ed|e)|destroyed|damaged|washed (away|out)|swept away|missing|injured|killed|fatalit(ies|y)|dead|displaced|evacuate(d|s|ing)?|rescued|stranded|trapped)\b/i;

const CONTEXT_WEATHER_RE =
  /\b(rain|snow|storm|wind|hail|lightning|flood|earthquake|wildfire|mudslide|landslide|tsunami|volcano|heat wave|drought|monsoon|typhoon|cyclone|hurricane)\b/i;

const GENERIC_IMPACT_LABEL = "Significant Impact Event";
const GENERIC_POTENTIAL_LABEL = "Potential Impact Event";

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
  goldstein,
  avgTone,
}) {
  const combined = [actor1, actor2, place, url, domain]
    .join(" ")
    .replace(/[-_/]+/g, " ")
    .toLowerCase();

  // 1) Direct hazard match from patterns
  const direct = detectHazardFromText(combined);
  if (direct) return direct;

  // 2) Explicit damage / casualties language
  if (DAMAGE_PATTERN.test(combined)) return GENERIC_IMPACT_LABEL;

  // 3) Root / event codes hinting at disaster/emergency
  if (HAZARD_ROOTCODES.has(rootCode) || HAZARD_EVENTCODES.has(eventCode)) {
    // If we see any weather/disaster context, upgrade label
    if (CONTEXT_WEATHER_RE.test(combined)) {
      return "Disaster / Emergency Event";
    }
    return GENERIC_IMPACT_LABEL;
  }

  // 4) Fallback: strongly negative Goldstein/AvgTone + disaster-ish context
  const g = Number(goldstein);
  const tone = Number(avgTone);
  const stronglyNegative = g <= -3 || tone <= -2;

  if (stronglyNegative && CONTEXT_WEATHER_RE.test(combined)) {
    return GENERIC_POTENTIAL_LABEL;
  }

  // 5) Otherwise, no hazard
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
      bonus: 0.4,
    },
    {
      method: "actor1",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      bonus: 0.2,
    },
    {
      method: "actor2",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      bonus: 0.1,
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
// Title / summary helpers
// ---------------------------------------------------------------------------

function nicePlaceName(raw) {
  if (!raw) return "Unknown location";
  // Take first part before comma to keep it short
  const s = String(raw).split(",")[0].trim();
  return s || "Unknown location";
}

function buildTitle(hazardLabel, place) {
  const loc = nicePlaceName(place);
  return `${hazardLabel} near ${loc}`;
}

function buildDescription(hazardLabel, place) {
  const loc = nicePlaceName(place);
  return `${hazardLabel} reported near ${loc}.`;
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

   console.log("🧪 GDELT using DB:", db.databaseName);

   const col = db.collection("social_signals");

    // TTL cleanup for GDELT docs only
    //await col.deleteMany({ source: "GDELT", expires: { $lte: now } });

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
      const goldstein = Number(cols[IDX.GOLDSTEIN]);
      const avgTone = Number(cols[IDX.AVGTONE]);

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place = geo.fullName || "";

      let hazardLabel = classifyHazard({
        actor1,
        actor2,
        place,
        url,
        domain,
        eventCode,
        rootCode,
        goldstein,
        avgTone,
      });

      // 🚨 NEVER DROP A GEO-LOCATED ARTICLE
      if (!hazardLabel) {
        hazardLabel = "News Event";
      }

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

      const title = buildTitle(hazardLabel, place);
      const description = buildDescription(hazardLabel, place);

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel,
        domain,
        url,

        title,
        description,

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
          goldstein,
          avgTone,
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

     const finalCount = await col.countDocuments({ source: "GDELT" });
     console.log("🧪 GDELT docs currently in social_signals:", finalCount);

  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  }
}
