// src/services/gdeltPoller.mjs
// GDELT Poller — working, stable, TLS-bypass enabled
//
// Fixes:
// - Correct 61-col schema (SOURCEURL = column 60)
// - Smart geo selection (ActionGeo > Actor1 > Actor2)
// - Avoid generic "United States" center-point
// - Hazard detection massively upgraded
// - TLS certificate bypass (Render-compatible)
// - US-only filter default ON (env override)
// - Batching + TTL + capped ingestion
// - 100% drop-in compatible with existing backend

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import https from "https";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

const httpsAgent = new https.Agent({ rejectUnauthorized: false });

// GDELT Schema (0..60)
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
  "tmz.com","people.com","variety.com","hollywoodreporter.com",
  "perezhilton","eonline.com","buzzfeed.com","usmagazine.com",
  "entertainment"
];

// US-only filter ON by default
const US_ONLY = String(process.env.GDELT_US_ONLY ?? "true").toLowerCase() !== "false";

const US_COUNTRY_CODES = new Set([
  "US","USA","AS","GU","MP","PR","VI","AQ","GQ","CQ","RQ","VQ"
]);

// TTL
const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 3600 * 1000;

function looksLikeUSBounds(lon, lat) {
  const conus = lon>=-125 && lon<=-66 && lat>=24 && lat<=50;
  const ak = lon>=-170 && lon<=-130 && lat>=50 && lat<=72;
  const hi = lon>=-161 && lon<=-154 && lat>=18 && lat<=23;
  const pr = lon>=-67.5 && lon<=-65 && lat>=17 && lat<=19;
  return conus || ak || hi || pr;
}

function shouldKeepUS(placeCountry, lon, lat) {
  if (!US_ONLY) return true;
  const cc = String(placeCountry || "").trim().toUpperCase();
  if (cc && US_COUNTRY_CODES.has(cc)) return true;
  return looksLikeUSBounds(lon, lat);
}

function isValidLonLat(lon, lat) {
  return Number.isFinite(lon) && Number.isFinite(lat)
    && lat>=-90 && lat<=90 && lon>=-180 && lon<=180;
}

function parseSqlDate(sqlDate) {
  if (!/^\d{8}$/.test(sqlDate)) return null;
  return new Date(`${sqlDate.slice(0,4)}-${sqlDate.slice(4,6)}-${sqlDate.slice(6)}T00:00:00Z`);
}

function parseDateAdded(added) {
  const m = added?.match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
  if (!m) return null;
  const [_,Y,Mo,D,h,mi,s]=m;
  return new Date(Date.UTC(+Y,+Mo-1,+D,+h,+mi,+s));
}

const coarsePlace = /^(united states|usa|canada|mexico)$/i;

function scoreGeo(c) {
  let s = 0;

  const t = parseInt(c.type, 10);
  if (Number.isFinite(t)) s += Math.min(10, Math.max(0, t));

  if (c.adm1) s += 3;
  if (c.adm2) s += 2;

  const n = String(c.fullName || "").trim();
  if (n) {
    const parts = n.split(",").map(p=>p.trim());
    s += Math.min(4, parts.length);
    if (parts.length === 1 && coarsePlace.test(parts[0])) s -= 8;
  } else {
    s -= 4;
  }

  return s + (c.baseBonus || 0);
}

function pickBestCoords(cols) {
  const cands = [
    {
      method:"gdelt-action-geo",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      type: cols[IDX.ACTIONGEO_TYPE],
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      baseBonus: 0.3
    },
    {
      method:"gdelt-actor1-geo",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      type: cols[IDX.ACTOR1GEO_TYPE],
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      baseBonus: 0.1
    },
    {
      method:"gdelt-actor2-geo",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      type: cols[IDX.ACTOR2GEO_TYPE],
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      baseBonus: 0
    }
  ];

  let best = null;
  for (const c of cands) {
    if (!isValidLonLat(c.lon, c.lat)) continue;
    const sc = scoreGeo(c);
    if (!best || sc > best.sc) best = {...c, sc};
  }

  return best && {
    lon: best.lon,
    lat: best.lat,
    method: best.method,
    place: String(best.fullName || "").trim(),
    placeCountry: String(best.country || "")
  };
}

function getDomain(url) {
  try { return new URL(url).hostname.replace(/^www\./,""); }
  catch { return ""; }
}

function hazardHaystack(parts) {
  return parts.filter(Boolean).join(" ").replace(/[_/.:-]+/g," ").toLowerCase();
}

const HAZARD_PATTERNS = [
  { label:"Tornado", re:/\b(tornado|twister|waterspout)\b/i },
  { label:"Hurricane / Tropical", re:/\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },
  { label:"High wind", re:/\b(high winds?|strong winds?|damaging winds?|windstorm|gusts?)\b/i },
  { label:"Severe storm", re:/\b(thunderstorm|severe storm|damaging wind|hail)\b/i },
  { label:"Winter storm", re:/\b(blizzard|winter storm|snowstorm|snow squall|whiteout|ice storm)\b/i },
  { label:"Flood", re:/\b(flood|flash flood|flooding|inundation|storm surge)\b/i },
  { label:"Wildfire", re:/\b(wildfire|forest fire|brush fire|grass fire)\b/i },
  { label:"Earthquake", re:/\b(earthquake|aftershock|seismic)\b/i },
  { label:"Landslide", re:/\b(landslide|mudslide|debris flow)\b/i },
  { label:"Extreme heat", re:/\b(heat wave|extreme heat|dangerous heat|excessive heat)\b/i },
  { label:"Drought", re:/\b(drought|water shortage)\b/i },
  { label:"Power outage", re:/\b(power outage|blackout|without power|no power|loss of power|downed lines?)\b/i },
  { label:"Hazmat / Explosion", re:/\b(hazmat|chemical spill|gas leak|explosion)\b/i }
];

function detectHazardLabel(text) {
  for (const {label,re} of HAZARD_PATTERNS)
    if (re.test(text)) return label;
  return null;
}

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 300;
  const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 200;

  let scanned=0, urlOK=0, geoOK=0, usKept=0, hazOK=0, saved=0;

  const now = new Date();

  try {
    // Fetch lastupdate.txt with TLS bypass
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      httpsAgent, timeout:15000, headers:{ "User-Agent": USER_AGENT }
    });

    const lines = String(lastFile).split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l)=>/\d{14}\.export\.CSV\.zip$/.test(l));
    if (!zipLine) {
      console.warn("⚠️ No GDELT export found");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Download:", zipUrl);

    const zipResp = await axios.get(zipUrl, {
      httpsAgent, responseType:"stream", timeout:60000,
      headers:{ "User-Agent": USER_AGENT }
    });

    const directory = zipResp.data.pipe(unzipper.Parse({forceStream:true}));
    let csvStream = null;

    for await (const entry of directory) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry; break;
      }
      entry.autodrain();
    }
    if (!csvStream) {
      console.warn("⚠️ ZIP had no CSV");
      return;
    }

    console.log("📄 Parsing CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const col = getDB().collection("social_signals");

    // TTL cleanup
    try {
      await col.deleteMany({ source:"GDELT", expires:{ $lte: now } });
    } catch {}

    let bulk = [];

    const flush = async ()=> {
      if (!bulk.length) return;
      try { await col.bulkWrite(bulk,{ordered:false}); }
      catch(e){ console.warn("bulkWrite warn:", e.message); }
      finally { bulk = []; }
    };

    for await (const line of rl) {
      scanned++;
      if (!line.trim()) continue;

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;
      urlOK++;

      const domain = getDomain(url);
      if (!domain || BLOCKED_DOMAIN_SUBSTRINGS.some(b=>domain.includes(b))) continue;

      const coords = pickBestCoords(cols);
      if (!coords) continue;
      geoOK++;

      if (!shouldKeepUS(coords.placeCountry, coords.lon, coords.lat)) continue;
      usKept++;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";

      const place = coords.place || "Unknown location";

      const hay = hazardHaystack([actor1, actor2, place, coords.placeCountry, url, domain]);
      const hz = detectHazardLabel(hay);
      if (!hz) continue;
      hazOK++;

      const sql = cols[IDX.SQLDATE];
      const added = cols[IDX.DATEADDED];
      const publishedAt =
        parseDateAdded(added) ||
        parseSqlDate(sql) ||
        now;

      const expires = new Date(publishedAt.getTime()+TTL_MS);

      const doc = {
        type:"news",
        source:"GDELT",
        hazardLabel: hz,
        domain, url,

        title: `${hz} near ${place}`,
        description: `${hz} reported near ${place}.`,

        publishedAt,
        updatedAt: now,
        expires,

        geometry: { type:"Point", coordinates:[coords.lon, coords.lat] },
        location: { type:"Point", coordinates:[coords.lon, coords.lat] },
        geometryMethod: coords.method,
        lat: coords.lat,
        lon: coords.lon,

        gdelt:{
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate: sql,
          dateAdded: added,
          rootCode: cols[IDX.EVENTROOTCODE],
          eventCode: cols[IDX.EVENTCODE],
          goldstein: Number(cols[IDX.GOLDSTEIN]),
          avgTone: Number(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          place,
          placeCountry: coords.placeCountry
        }
      };

      bulk.push({
        updateOne:{
          filter:{ url },
          update:{ $set: doc, $setOnInsert:{ createdAt:now } },
          upsert:true
        }
      });

      saved++;
      if (bulk.length >= BATCH_SIZE) await flush();
      if (saved >= MAX_SAVE) break;
    }

    await flush();

    console.log(`🌍 GDELT DONE — scanned=${scanned}, urlOK=${urlOK}, geoOK=${geoOK}, USkept=${usKept}, hazard=${hazOK}, saved=${saved}`);

  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}

