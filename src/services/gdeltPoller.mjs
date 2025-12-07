// src/services/gdeltPoller.mjs
// GDELT Poller — “news layer actually shows up” version
// - Streams latest GDELT 2.0 Events export from lastupdate.txt
// - Uses correct 61-column Events schema (SOURCEURL is column 60)
// - Picks the BEST geo candidate (ActionGeo/Actor1/Actor2) by specificity (Geo_Type + ADM1/ADM2 + name),
//   avoiding “United States center-point” junk placements.
// - Broader hazard keyword coverage (windstorm / high winds / without power / outages / etc.)
// - Optional US-only filter (default ON) to keep the map relevant
// - Batches Mongo upserts, caps saved docs per run (env GDELT_MAX_SAVE)

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

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

// Optional (noise filter). Default OFF because it can hide relevant weather/outage stories.
const FILTER_ROOT_CODES =
  String(process.env.GDELT_FILTER_ROOT_CODES || "false").toLowerCase() === "true";

const ALLOWED_ROOT_CODES = new Set([
  "01", "02", "03", "04", "07", "08",
  "10", "11", "12",
  "14", "15", "16", "17", "18", "19", "20",
]);

// Default ON: keep the feed relevant for the app.
const US_ONLY = String(process.env.GDELT_US_ONLY ?? "true").toLowerCase() !== "false";

// Country codes here are messy across feeds (FIPS/ISO). Support both common variants.
// US territories in FIPS: AQ(AS), GQ(GU), CQ(MP), RQ(PR), VQ(VI)
const US_COUNTRY_CODES = new Set([
  "US", "USA",
  "AQ", "GQ", "CQ", "RQ", "VQ",
  "AS", "GU", "MP", "PR", "VI",
]);

// TTL for news docs (hours). Default 24.
const TTL_HOURS = (() => {
  const n = Number.parseFloat(process.env.GDELT_TTL_HOURS);
  return Number.isFinite(n) && n > 0 ? n : 24;
})();
const TTL_MS = TTL_HOURS * 60 * 60 * 1000;

// Hazard patterns (ordered; first match wins)
const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },

  // Winds / storms (this one fixes “windstorm” not matching)
  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|wind event|gusts?|gusty|wind advisory|wind warning)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe weather|severe storm|damaging wind|hail|microburst|downburst)\b/i },

  // Winter (add whiteout + blowing snow)
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|snow squall|whiteout|blowing snow|ice storm|freezing rain|freezing drizzle)\b/i },

  { label: "Flood", re: /\b(flash flood|flooding|flood|inundat(?:e|ion)|storm surge)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire|bush ?fire)\b/i },

  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|volcanic|lava flow|ash plume)\b/i },
  { label: "Avalanche", re: /\b(avalanche)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow|rockslide)\b/i },

  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|record heat|excessive heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage|water crisis)\b/i },

  // Outages (this fixes “without power” not matching)
  { label: "Power outage", re: /\b(power(?:\s|-)?outage|blackout|outages?|without power|no power|loss of power|widespread outage|downed (?:power )?lines?)\b/i },

  { label: "Hazmat / Explosion", re: /\b(hazmat|chemical spill|toxic leak|gas leak|industrial accident|explosion)\b/i },
];

function detectHazardLabel(text = "") {
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

function parseSqlDate(sqlDate) {
  // SQLDATE is YYYYMMDD
  const s = String(sqlDate || "").trim();
  if (!/^\d{8}$/.test(s)) return null;
  return new Date(`${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6)}T00:00:00Z`);
}

function parseDateAdded(dateAdded) {
  // DATEADDED is YYYYMMDDHHMMSS
  const s = String(dateAdded || "").trim();
  const m = s.match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
  if (!m) return null;
  const [_, Y, Mo, D, h, mi, se] = m;
  return new Date(Date.UTC(+Y, +Mo - 1, +D, +h, +mi, +se));
}

const COARSE_PLACE_RE = /^(united states|usa|canada|mexico)$/i;

function scoreGeoCandidate(c) {
  let score = 0;

  // Geo_Type tends to increase with specificity (country < admin < city).
  const t = Number.parseInt(String(c.type ?? ""), 10);
  if (Number.isFinite(t)) score += Math.max(0, Math.min(10, t));

  if (c.adm1) score += 2;
  if (c.adm2) score += 2;

  const name = String(c.fullName || "").trim();
  if (name) {
    const parts = name.split(",").map((x) => x.trim()).filter(Boolean);
    score += Math.min(4, parts.length); // more commas => more specific
    if (parts.length === 1 && COARSE_PLACE_RE.test(parts[0])) score -= 6; // avoid country-centroid pins
  } else {
    score -= 3;
  }

  if (c.country) score += 1;

  // small tie-breaker to prefer ActionGeo if equally specific
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
      baseBonus: 0.25,
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
      baseBonus: 0,
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
      baseBonus: -0.1,
    },
  ];

  let best = null;

  for (const c of candidates) {
    if (!isValidLonLat(c.lon, c.lat)) continue;
    const s = scoreGeoCandidate(c);
    if (!best || s > best.score) best = { ...c, score: s };
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

function isBlockedDomain(domain) {
  if (!domain) return true;
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => domain.includes(b));
}

// If country codes are missing, this helps keep only US-ish points.
// (Rough bounds for CONUS + AK + HI + PR)
function looksLikeUSBounds(lon, lat) {
  const inCONUS = lon >= -125 && lon <= -66 && lat >= 24 && lat <= 50;
  const inAK = lon >= -170 && lon <= -130 && lat >= 50 && lat <= 72;
  const inHI = lon >= -161 && lon <= -154 && lat >= 18 && lat <= 23;
  const inPR = lon >= -67.5 && lon <= -65 && lat >= 17 && lat <= 19;
  return inCONUS || inAK || inHI || inPR;
}

function shouldKeepUS(placeCountry, lon, lat) {
  if (!US_ONLY) return true;
  const cc = String(placeCountry || "").trim().toUpperCase();
  if (cc && US_COUNTRY_CODES.has(cc)) return true;
  return looksLikeUSBounds(lon, lat);
}

function hazardHaystack(parts) {
  const raw = parts.filter(Boolean).join(" ");
  // Help regex word boundaries match URL slugs and punctuation
  return raw.replace(/[_/\\.-]+/g, " ").toLowerCase();
}

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
    // 1) Determine latest events export file
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const lines = String(lastFile).split(/\r?\n/).filter(Boolean);

    // Grab the most recent export.CSV.zip line (usually already the first match)
    const zipLine = lines.find((l) => /\d{14}\.export\.CSV\.zip$/.test(l));
    if (!zipLine) {
      console.warn("⚠️ No GDELT export.CSV.zip found in lastupdate.txt");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading:", zipUrl);

    // 2) Download zip stream
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    // 3) Extract the .CSV inside
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

    // Opportunistic cleanup (safe: only touches GDELT docs)
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
        console.warn("⚠️ bulkWrite warning:", e?.message || e);
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

      const placeCountry =
        coords.placeCountry ||
        cols[IDX.ACTIONGEO_COUNTRY] ||
        cols[IDX.ACTOR1GEO_COUNTRY] ||
        cols[IDX.ACTOR2GEO_COUNTRY] ||
        "";

      const haystack = hazardHaystack([actor1, actor2, place, placeCountry, url, domain, rootCode]);
      const hazardLabel = detectHazardLabel(haystack);
      if (!hazardLabel) continue;
      hazardMatched++;

      const sqlDate = cols[IDX.SQLDATE];
      const dateAdded = cols[IDX.DATEADDED];
      const publishedAt = parseDateAdded(dateAdded) || parseSqlDate(sqlDate) || now;
      const expires = new Date(publishedAt.getTime() + TTL_MS);

      // Debug first 10 kept rows
      if (debugPrinted < 10) {
        console.log("GDELT KEEP:", {
          hazardLabel,
          place,
          placeCountry,
          domain,
          lon: coords.lon,
          lat: coords.lat,
          geoMethod: coords.method,
          rootCode,
          url,
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
        description: `${hazardLabel} reported near ${place}${placeCountry ? ` (${placeCountry})` : ""}.`,

        publishedAt,
        updatedAt: now,
        expires,

        // Keep BOTH for compatibility (different endpoints/clients may use either)
        geometry: { type: "Point", coordinates: [coords.lon, coords.lat] },
        location: { type: "Point", coordinates: [coords.lon, coords.lat] },
        lat: coords.lat,
        lon: coords.lon,
        geometryMethod: coords.method,

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
          placeCountry,
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
      `🌍 GDELT DONE — scanned=${scanned}, withUrl=${withUrl}, withCoords=${withCoords}, keptUS=${keptUS}, hazardMatched=${hazardMatched}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  }
}
