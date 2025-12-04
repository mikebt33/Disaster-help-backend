// src/services/gdeltPoller.mjs
// GDELT Poller — MVP "actually works" version
// - Streams latest GDELT 2.0 Events export from lastupdate.txt
// - Uses the CORRECT 61-column Events schema (SOURCEURL is column 60)
// - Prefers ActionGeo coords, falls back to Actor1Geo, then Actor2Geo
// - Validates lat/lon hard
// - Batches Mongo upserts (fast + resilient)
// - Limits saved docs per poll (env GDELT_MAX_SAVE) to avoid DB bloat

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

// GDELT Events 2.0 schema is 61 columns (0..60):
// ... ActionGeo_Lat=56, ActionGeo_Long=57, DATEADDED=59, SOURCEURL=60
const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1GEO_FULLNAME: 36,
  ACTOR1GEO_COUNTRY: 37,
  ACTOR1GEO_LAT: 40,
  ACTOR1GEO_LON: 41,

  ACTOR2GEO_FULLNAME: 44,
  ACTOR2GEO_COUNTRY: 45,
  ACTOR2GEO_LAT: 48,
  ACTOR2GEO_LON: 49,

  ACTIONGEO_FULLNAME: 52,
  ACTIONGEO_COUNTRY: 53,
  ACTIONGEO_LAT: 56,
  ACTIONGEO_LON: 57,

  DATEADDED: 59,
  SOURCEURL: 60,
});

const MIN_COLUMNS = 61;

const BLOCKED_DOMAINS = [
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

// Keep your expanded root codes (optional filter, but it does cut noise)
const ALLOWED_ROOT_CODES = new Set([
  "01", "02", "03", "04", "07", "08",
  "10", "11", "12",
  "14", "15", "16", "17", "18", "19", "20",
]);

// Hazard patterns (keep it fairly permissive; URL slugs are your friend)
const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe weather|severe storm|damaging wind|hail|gusts?)\b/i },
  { label: "Flood", re: /\b(flash flood|flooding|flood|storm surge)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|snow squall|ice storm|freezing rain|freezing drizzle)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire|bush ?fire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage|water crisis)\b/i },
  { label: "Power outage", re: /\b(power outage|blackout|loss of power|widespread outage)\b/i },
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

function pickBestCoords(cols) {
  // Prefer ActionGeo
  const aLat = parseFloat(cols[IDX.ACTIONGEO_LAT]);
  const aLon = parseFloat(cols[IDX.ACTIONGEO_LON]);
  if (isValidLonLat(aLon, aLat)) {
    return { lon: aLon, lat: aLat, method: "gdelt-action-geo" };
  }

  // Fallback Actor1Geo
  const a1Lat = parseFloat(cols[IDX.ACTOR1GEO_LAT]);
  const a1Lon = parseFloat(cols[IDX.ACTOR1GEO_LON]);
  if (isValidLonLat(a1Lon, a1Lat)) {
    return { lon: a1Lon, lat: a1Lat, method: "gdelt-actor1-geo" };
  }

  // Fallback Actor2Geo
  const a2Lat = parseFloat(cols[IDX.ACTOR2GEO_LAT]);
  const a2Lon = parseFloat(cols[IDX.ACTOR2GEO_LON]);
  if (isValidLonLat(a2Lon, a2Lat)) {
    return { lon: a2Lon, lat: a2Lat, method: "gdelt-actor2-geo" };
  }

  return null;
}

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  const MAX_SAVE = Number.isFinite(Number(process.env.GDELT_MAX_SAVE))
    ? Number(process.env.GDELT_MAX_SAVE)
    : 300;

  const BATCH_SIZE = 200;

  let scanned = 0;
  let withUrl = 0;
  let withCoords = 0;
  let hazardMatched = 0;
  let saved = 0;

  try {
    // 1) Determine latest events export file
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": "disaster-help-backend/1.0" },
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
      headers: { "User-Agent": "disaster-help-backend/1.0" },
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
      if (!domain) continue;
      if (BLOCKED_DOMAINS.some((b) => domain.includes(b))) continue;

      const rootCode = String(cols[IDX.EVENTROOTCODE] || "").trim();
      if (rootCode && !ALLOWED_ROOT_CODES.has(rootCode)) continue;

      const coords = pickBestCoords(cols);
      if (!coords) continue;
      withCoords++;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const actionPlace = cols[IDX.ACTIONGEO_FULLNAME] || cols[IDX.ACTOR1GEO_FULLNAME] || cols[IDX.ACTOR2GEO_FULLNAME] || "";
      const hazardText = `${actor1} ${actor2} ${actionPlace} ${url}`.toLowerCase();

      const hazardLabel = detectHazardLabel(hazardText);
      if (!hazardLabel) continue;
      hazardMatched++;

      const sqlDate = cols[IDX.SQLDATE];
      const dateAdded = cols[IDX.DATEADDED];

      const publishedAt =
        parseDateAdded(dateAdded) ||
        parseSqlDate(sqlDate) ||
        new Date();

      const placeCountry = cols[IDX.ACTIONGEO_COUNTRY] || cols[IDX.ACTOR1GEO_COUNTRY] || cols[IDX.ACTOR2GEO_COUNTRY] || "";
      const place = actionPlace || "Unknown location";

      // Debug first 8 kept rows (so you can see it’s sane)
      if (debugPrinted < 8) {
        console.log("GDELT KEEP:", {
          rootCode,
          hazardLabel,
          place,
          placeCountry,
          domain,
          lon: coords.lon,
          lat: coords.lat,
          url,
        });
        debugPrinted++;
      }

      const doc = {
        type: "news",
        source: "GDELT",
        domain,
        url,

        title: `${hazardLabel} near ${place}`,
        description: `${hazardLabel} reported near ${place}${placeCountry ? ` (${placeCountry})` : ""}.`,

        publishedAt,
        createdAt: new Date(),
        // Keep short-lived; your server already has TTL-style cleanup logic elsewhere.
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000),

        geometry: { type: "Point", coordinates: [coords.lon, coords.lat] },
        geometryMethod: coords.method,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate,
          dateAdded,
          rootCode,
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
          update: { $set: doc },
          upsert: true,
        },
      });

      saved++;

      if (bulkOps.length >= BATCH_SIZE) await flush();

      // Cap per run, so we don’t write 100k documents and ruin your day
      if (saved >= MAX_SAVE) {
        console.log(`🧯 Reached GDELT_MAX_SAVE=${MAX_SAVE}, stopping early.`);
        break;
      }
    }

    await flush();

    console.log(
      `🌍 GDELT DONE — scanned=${scanned}, withUrl=${withUrl}, withCoords=${withCoords}, hazardMatched=${hazardMatched}, saved=${saved}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  }
}
