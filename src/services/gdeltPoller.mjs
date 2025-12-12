import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL =
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT =
  process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 3600 * 1000;

const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 500;
const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 200;
const MAX_RUNTIME_MS =
  Number(process.env.GDELT_MAX_RUNTIME_MS) || 7 * 60 * 1000;
const FLUSH_EVERY_MS = Number(process.env.GDELT_FLUSH_EVERY_MS) || 8000;

const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "perezhilton",
  "hollywoodreporter.com",
  "eonline.com",
  "usmagazine.com",
  "buzzfeed.com",
];

const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTCODE: 26,
  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1GEO_FULLNAME: 36,
  ACTOR1GEO_COUNTRY: 37,
  ACTOR1GEO_ADM1: 38,
  ACTOR1GEO_ADM2: 39,
  ACTOR1GEO_LAT: 40,
  ACTOR1GEO_LON: 41,

  ACTOR2GEO_FULLNAME: 44,
  ACTOR2GEO_COUNTRY: 45,
  ACTOR2GEO_ADM1: 46,
  ACTOR2GEO_ADM2: 47,
  ACTOR2GEO_LAT: 48,
  ACTOR2GEO_LON: 49,

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

const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  {
    label: "Hurricane / Tropical",
    re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i,
  },
  {
    label: "Severe Storm",
    re: /\b(thunderstorm|severe storm|hail|microburst|downburst|squall)\b/i,
  },
  {
    label: "High Wind",
    re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusty)\b/i,
  },
  {
    label: "Winter Storm",
    re: /\b(blizzard|winter storm|snowstorm|whiteout|ice storm)\b/i,
  },
  {
    label: "Flood",
    re: /\b(flood(ing)?|flash flood|inundat|storm surge|levee|dam burst)\b/i,
  },
  {
    label: "Wildfire",
    re: /\b(wild ?fire|forest fire|brush fire|grass fire|bushfire)\b/i,
  },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|eruption|lava|ash plume)\b/i },
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
    re: /\b(power outage|blackout|grid failure|downed lines?)\b/i,
  },
  {
    label: "Explosion / Hazmat",
    re: /\b(explosion|blast|hazmat|chemical spill|toxic leak|gas leak)\b/i,
  },
];

const DAMAGE_RE =
  /\b(damage(d)?|destroy(ed)?|collapsed|washed (away|out)|missing|injured|killed|fatalit(ies|y)|dead|displaced|evacuat(e|ed|ing)|rescued|stranded|trapped)\b/i;

const DISASTER_CONTEXT_RE =
  /\b(rain|snow|storm|wind|hail|lightning|flood|earthquake|wildfire|mudslide|landslide|tsunami|volcano|heat wave|drought|monsoon|typhoon|cyclone|hurricane)\b/i;

function rewriteGdeltUrl(url) {
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

function isBlocked(domain) {
  if (!domain) return false;
  const d = domain.toLowerCase();
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => d.includes(b));
}

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
  const cands = [
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
  for (const x of cands) {
    if (!validLonLat(x.lon, x.lat)) continue;

    let score = x.bonus;
    if (x.adm1) score += 1.5;
    if (x.adm2) score += 1.0;
    if (x.fullName) score += Math.min(4, String(x.fullName).split(",").length);

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

function nicePlaceName(raw) {
  if (!raw) return "Unknown location";
  const s = String(raw).split(",")[0].trim();
  return s || "Unknown location";
}

function parseDateAdded(dateAdded, fallback) {
  if (!dateAdded || !/^\d{14}$/.test(dateAdded)) return fallback;
  const Y = dateAdded.slice(0, 4);
  const M = dateAdded.slice(4, 6);
  const D = dateAdded.slice(6, 8);
  const h = dateAdded.slice(8, 10);
  const m = dateAdded.slice(10, 12);
  const s = dateAdded.slice(12, 14);
  const d = new Date(`${Y}-${M}-${D}T${h}:${m}:${s}Z`);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

function detectHazard(text) {
  if (!text) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

let GDELT_RUNNING = false;

export async function pollGDELT() {
  if (GDELT_RUNNING) return;
  GDELT_RUNNING = true;

  const startedAt = Date.now();
  const deadline = startedAt + MAX_RUNTIME_MS;

  let zipStream = null;
  let csvStream = null;
  let rl = null;

  let scanned = 0;
  let queued = 0;

  try {
    const { data: lastTxt } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const line = String(lastTxt)
      .split(/\r?\n/)
      .find((l) => l.includes(".export.CSV.zip"));

    if (!line) return;

    const originalUrl = line.trim().split(/\s+/).pop();
    const zipUrl = rewriteGdeltUrl(originalUrl);

    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    zipStream = zipResp.data;
    csvStream = zipStream.pipe(unzipper.ParseOne(/\.csv$/i));
    rl = readline.createInterface({ input: csvStream, crlfDelay: Infinity });

    const db = getDB();
    const col = db.collection("social_signals");

    let bulk = [];
    let lastFlushAt = Date.now();

    async function flushBulk() {
      if (!bulk.length) return;
      try {
        await col.bulkWrite(bulk, { ordered: false });
      } catch (err) {
        console.error("❌ GDELT bulkWrite error:", err?.message || err);
        if (err?.writeErrors?.length) {
          console.error("❌ First writeError:", err.writeErrors[0]?.errmsg);
        }
      } finally {
        bulk = [];
        lastFlushAt = Date.now();
      }
    }

    for await (const line of rl) {
      if (Date.now() > deadline) break;
      if (!line) continue;

      scanned++;
      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (isBlocked(domain)) continue;

      const geo = pickGeo(cols);
      if (!geo) continue;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place = geo.fullName || "";

      const combined = [actor1, actor2, place, url, domain]
        .join(" ")
        .replace(/[-_/]+/g, " ")
        .toLowerCase();

      let hazardLabel = detectHazard(combined);

      if (!hazardLabel) {
        if (DAMAGE_RE.test(combined) && DISASTER_CONTEXT_RE.test(combined)) {
          hazardLabel = "Disaster / Emergency";
        } else {
          continue;
        }
      }

      const now = new Date();
      const dateAdded = cols[IDX.DATEADDED];
      const publishedAt = parseDateAdded(dateAdded, now);
      const expires = new Date(Date.now() + TTL_MS);

      const [jLon, jLat] = microJitter(
        geo.lon,
        geo.lat,
        `${cols[IDX.GLOBALEVENTID]}|${url}`,
        0.12
      );

      const loc = nicePlaceName(place);
      const summary = `${hazardLabel} near ${loc}`;

      const doc = {
        type: "news",
        source: "GDELT",
        provider: "GDELT",
        hazardLabel,
        domain,
        url,
        title: summary,
        summary,
        description: summary,
        publishedAt,
        updatedAt: now,
        expires,
        geometry: { type: "Point", coordinates: [jLon, jLat] },
        lat: jLat,
        lon: jLon,
        geometryMethod: `gdelt-${geo.method}`,
        place,
        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate: cols[IDX.SQLDATE],
          dateAdded,
          eventCode: String(cols[IDX.EVENTCODE] || "").trim(),
          rootCode: String(cols[IDX.EVENTROOTCODE] || "").trim(),
          goldstein: Number(cols[IDX.GOLDSTEIN]),
          avgTone: Number(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          country: geo.country || "",
          geoMethod: geo.method,
        },
      };

      bulk.push({
        updateOne: {
          filter: { url },
          update: { $set: doc, $setOnInsert: { createdAt: now } },
          upsert: true,
        },
      });

      queued++;

      if (bulk.length >= BATCH_SIZE || Date.now() - lastFlushAt > FLUSH_EVERY_MS) {
        await flushBulk();
      }

      if (queued >= MAX_SAVE) break;
    }

    await flushBulk();
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  } finally {
    GDELT_RUNNING = false;
    try {
      rl?.close();
    } catch {}
    try {
      csvStream?.destroy();
    } catch {}
    try {
      zipStream?.destroy();
    } catch {}
    const runtimeSec = Math.round((Date.now() - startedAt) / 1000);
    console.log(`✅ GDELT done scanned=${scanned} saved=${queued} runtimeSec=${runtimeSec}`);
  }
}
