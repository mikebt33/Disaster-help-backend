// src/services/gdeltPoller.mjs
// GDELT Poller — “make it show up” mode (reliable finish + visible writes)
//
// Key fixes:
// - HARD overlap guard with finally() release
// - Explicit readline + stream cleanup
// - Runtime cap so it can’t run forever on slow instances
// - Periodic bulk flush so writes happen quickly (not just at the end)
// - Geo fallback: ActionGeo -> Actor1Geo -> Actor2Geo
// - TTL anchored to NOW (prevents immediate TTL deletion if DATEADDED is old)

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

// -----------------------------
// CONFIG
// -----------------------------
const LASTUPDATE_URL =
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT =
  process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 3600 * 1000;

// Keep these low-ish while debugging so you SEE docs quickly
const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 300;
const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 50;

// Force exit so cron doesn’t “stack forever”
const MAX_RUNTIME_MS = Number(process.env.GDELT_MAX_RUNTIME_MS) || 9 * 60 * 1000;

// Flush even if BATCH_SIZE isn’t reached (so you get visible writes quickly)
const FLUSH_EVERY_MS = Number(process.env.GDELT_FLUSH_EVERY_MS) || 5000;

// Progress logs
const PROGRESS_EVERY_LINES = Number(process.env.GDELT_PROGRESS_EVERY_LINES) || 50000;

const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "perezhilton",
  "hollywoodreporter.com",
  "eonline.com",
  "usmagazine.com",
  "buzzfeed.com",
];

function rewriteGdeltUrl(url) {
  return url.replace(
    /^https?:\/\/data\.gdeltproject\.org\//i,
    "https://storage.googleapis.com/data.gdeltproject.org/"
  );
}

// -----------------------------
// GDELT Schema (61 columns)
// -----------------------------
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

// -----------------------------
// Helpers
// -----------------------------
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

// Choose best geo among ActionGeo / Actor1Geo / Actor2Geo (highest “quality”)
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

// -----------------------------
// Overlap guard (CRITICAL)
// -----------------------------
let GDELT_RUNNING = false;

// -----------------------------
// MAIN
// -----------------------------
export async function pollGDELT() {
  if (GDELT_RUNNING) {
    console.log("⏭️ GDELT skipped (previous run still in progress)");
    return;
  }
  GDELT_RUNNING = true;

  const startedAt = Date.now();
  const deadline = startedAt + MAX_RUNTIME_MS;

  let zipStream = null;
  let csvStream = null;
  let rl = null;

  let scanned = 0;
  let queued = 0;
  let flushed = 0;

  console.log("🌎 GDELT Poller running…");

  try {
    // 1) lastupdate.txt
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

    // 2) Download ZIP stream
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    zipStream = zipResp.data;
    zipStream.on("error", (e) =>
      console.error("❌ GDELT zip stream error:", e?.message || e)
    );

    // 3) Extract the CSV entry stream
    // Using ParseOne keeps things simple (single CSV per export zip).
    csvStream = zipStream.pipe(unzipper.ParseOne(/\.csv$/i));
    csvStream.on("error", (e) =>
      console.error("❌ GDELT csv stream error:", e?.message || e)
    );

    console.log("📄 Parsing GDELT Events CSV…");

    // 4) Read lines
    rl = readline.createInterface({ input: csvStream, crlfDelay: Infinity });

    const db = getDB();
    console.log("🧪 GDELT using DB:", db.databaseName);

    const col = db.collection("social_signals");

    let bulk = [];
    let lastFlushAt = Date.now();

    async function flushBulk(reason) {
      if (!bulk.length) return;

      const ops = bulk.length;
      try {
        const r = await col.bulkWrite(bulk, { ordered: false });
        flushed += ops;
        console.log(
          `✅ GDELT bulkWrite(${reason}) ops=${ops} upserted=${r.upsertedCount} modified=${r.modifiedCount} matched=${r.matchedCount}`
        );
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

    // If the stream is stuck, this log helps you see it quickly
    const stuckTimer = setTimeout(() => {
      if (scanned === 0) {
        console.warn("⚠️ GDELT: no lines read after 15s (stream may be stuck)");
      }
    }, 15000);

    for await (const line of rl) {
      if (Date.now() > deadline) {
        console.warn("⏰ GDELT runtime cap reached — stopping this run.");
        break;
      }

      if (!line) continue;

      scanned++;
      if (scanned % PROGRESS_EVERY_LINES === 0) {
        console.log(
          `…GDELT progress scanned=${scanned} queued=${queued} flushed=${flushed}`
        );
      }

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (isBlocked(domain)) continue;

      const geo = pickGeo(cols);
      if (!geo) continue;

      const now = new Date();
      const dateAdded = cols[IDX.DATEADDED];
      const publishedAt = parseDateAdded(dateAdded, now);

      // TTL anchored to NOW so docs can’t vanish immediately
      const expires = new Date(Date.now() + TTL_MS);

      const [jLon, jLat] = microJitter(
        geo.lon,
        geo.lat,
        `${cols[IDX.GLOBALEVENTID]}|${url}`,
        0.12
      );

      const place = geo.fullName || "";

      // “Always show something” label — you can re-add hazard classification later
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
        expires,

        geometry: { type: "Point", coordinates: [jLon, jLat] },
        lat: jLat,
        lon: jLon,
        geometryMethod: `gdelt-${geo.method}`,

        // Optional top-level place, nice for debugging / UI
        place,

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
          country: geo.country || "",
          geoMethod: geo.method,
        },
      };

      bulk.push({
        updateOne: {
          filter: { url },
          update: {
            $set: doc,
            $setOnInsert: { createdAt: now },
          },
          upsert: true,
        },
      });

      queued++;

      // Flush by batch size
      if (bulk.length >= BATCH_SIZE) {
        await flushBulk("batch");
      } else if (Date.now() - lastFlushAt > FLUSH_EVERY_MS) {
        // Flush by time (so you see writes quickly)
        await flushBulk("timer");
      }

      if (queued >= MAX_SAVE) {
        console.log(`🧯 Reached MAX_SAVE=${MAX_SAVE}, stopping early.`);
        break;
      }
    }

    clearTimeout(stuckTimer);

    // final flush
    await flushBulk("final");

    // Ensure readline is closed
    try {
      rl?.close();
    } catch {}

    const finalCount = await col.countDocuments({ source: "GDELT" });
    console.log(
      `🌍 GDELT DONE scanned=${scanned} queued=${queued} flushed=${flushed} dbCount(GDELT)=${finalCount} runtimeSec=${Math.round(
        (Date.now() - startedAt) / 1000
      )}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  } finally {
    // CRITICAL: release overlap guard
    GDELT_RUNNING = false;

    // CRITICAL: cleanup streams so we don’t hang forever
    try {
      rl?.close();
    } catch {}
    try {
      csvStream?.destroy();
    } catch {}
    try {
      zipStream?.destroy();
    } catch {}

    console.log("✅ GDELT finished cleanly");
  }
}
