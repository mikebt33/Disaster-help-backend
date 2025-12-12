import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL =
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT =
  process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

function rewriteGdeltUrl(url) {
  return url.replace(
    /^https?:\/\/data\.gdeltproject\.org\//i,
    "https://storage.googleapis.com/data.gdeltproject.org/"
  );
}

const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,
  ACTOR1NAME: 6,
  ACTOR2NAME: 16,
  EVENTCODE: 26,
  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,
  ACTOR1GEO_LAT: 40,
  ACTOR1GEO_LON: 41,
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

// DEBUG-friendly defaults (so you see writes happen)
const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 300;
const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 25;

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

// IMPORTANT: do NOT auto-block missing/invalid domains (that can zero your feed)
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
  const actionLat = parseFloat(cols[IDX.ACTIONGEO_LAT]);
  const actionLon = parseFloat(cols[IDX.ACTIONGEO_LON]);
  if (validLonLat(actionLon, actionLat)) {
    return {
      method: "action",
      lat: actionLat,
      lon: actionLon,
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
    };
  }
  return null;
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

// ---- overlap guard (CRITICAL) ----
let GDELT_RUNNING = false;

export async function pollGDELT() {
  if (GDELT_RUNNING) {
    console.log("⏭️ GDELT skipped (previous run still in progress)");
    return;
  }
  GDELT_RUNNING = true;

  const startTs = Date.now();
  console.log("🌎 GDELT Poller running…");

  let scanned = 0;
  let queued = 0;
  let flushed = 0;

  try {
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

    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

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

    const rl = readline.createInterface({ input: csvStream, crlfDelay: Infinity });
    const db = getDB();
    console.log("🧪 GDELT using DB:", db.databaseName);

    const col = db.collection("social_signals");

    let bulk = [];

    async function flushBulk(reason) {
      if (!bulk.length) return;

      try {
        const r = await col.bulkWrite(bulk, { ordered: false });
        flushed += bulk.length;
        console.log(
          `✅ GDELT bulkWrite (${reason}) ops=${bulk.length} upserted=${r.upsertedCount} modified=${r.modifiedCount} matched=${r.matchedCount}`
        );
      } catch (err) {
        console.error("❌ GDELT bulkWrite error:", err?.message || err);
        if (err?.writeErrors?.length) {
          console.error("❌ First writeError:", err.writeErrors[0]?.errmsg);
        }
      } finally {
        bulk = [];
      }
    }

    for await (const l of rl) {
      if (!l) continue;

      scanned++;
      if (scanned % 50000 === 0) {
        console.log(`…GDELT progress scanned=${scanned} queued=${queued} flushed=${flushed}`);
      }

      const cols = l.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (isBlocked(domain)) continue;

      const geo = pickGeo(cols);
      if (!geo) continue;

      const now = new Date();

      // IMPORTANT: publish time can be old; keep it, but NEVER base TTL on it
      let publishedAt = now;
      const dateAdded = cols[IDX.DATEADDED];
      if (/^\d{14}$/.test(dateAdded)) {
        const Y = dateAdded.slice(0, 4);
        const M = dateAdded.slice(4, 6);
        const D = dateAdded.slice(6, 8);
        const h = dateAdded.slice(8, 10);
        const m = dateAdded.slice(10, 12);
        const s = dateAdded.slice(12, 14);
        const parsed = new Date(`${Y}-${M}-${D}T${h}:${m}:${s}Z`);
        if (!Number.isNaN(parsed.getTime())) publishedAt = parsed;
      }

      // ✅ TTL anchored to NOW (cannot instantly disappear)
      const expires = new Date(Date.now() + TTL_MS);

      const [jLon, jLat] = microJitter(
        geo.lon,
        geo.lat,
        `${cols[IDX.GLOBALEVENTID]}|${url}`,
        0.12
      );

      const place = geo.fullName || "";
      const hazardLabel = "News Event";

      const title = `${hazardLabel} near ${nicePlaceName(place)}`;
      const description = `${hazardLabel} reported near ${nicePlaceName(place)}.`;

      const doc = {
        type: "news",
        source: "GDELT",
        provider: "GDELT",
        hazardLabel,
        domain,
        url,
        title,
        description,
        publishedAt,
        updatedAt: now,
        createdAt: now,
        expires,
        geometry: { type: "Point", coordinates: [jLon, jLat] },
        lat: jLat,
        lon: jLon,
        geometryMethod: "gdelt-action",
        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate: cols[IDX.SQLDATE],
          dateAdded,
          eventCode: String(cols[IDX.EVENTCODE] || "").trim(),
          rootCode: String(cols[IDX.EVENTROOTCODE] || "").trim(),
          goldstein: Number(cols[IDX.GOLDSTEIN]),
          avgTone: Number(cols[IDX.AVGTONE]),
          actor1: cols[IDX.ACTOR1NAME] || "",
          actor2: cols[IDX.ACTOR2NAME] || "",
          place,
          country: geo.country,
        },
      };

      bulk.push({
        updateOne: {
          filter: { url },
          update: { $set: doc },
          upsert: true,
        },
      });

      queued++;

      if (bulk.length >= BATCH_SIZE) {
        await flushBulk("batch");
      }

      if (queued >= MAX_SAVE) {
        console.log(`🧯 Reached MAX_SAVE=${MAX_SAVE}, stopping early.`);
        break;
      }
    }

    await flushBulk("final");

    const finalCount = await col.countDocuments({ source: "GDELT" });
    console.log(
      `🌍 GDELT DONE scanned=${scanned} queued=${queued} flushed=${flushed} dbCount(GDELT)=${finalCount} runtimeSec=${Math.round(
        (Date.now() - startTs) / 1000
      )}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  } finally {
    GDELT_RUNNING = false;
  }
}
