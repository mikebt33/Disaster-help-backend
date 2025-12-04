// src/services/capPoller.mjs
//
// Unified Official Alert Poller
// - NWS / NOAA Weather API (/alerts)  ← replaces old alerts.weather.gov CAP feeds
// - FEMA IPAWS CAP (if reachable)
// - USGS Earthquake Atom feed
//
// Normalizes alerts into a common schema and saves to MongoDB collection: alerts_cap
// Adds deterministic "anti-stacking" jitter (miles) ONLY for fallback-based points
// (state / county centroid fallbacks), never for true polygon-derived points.
//
// Key fixes:
// 1) Prevent NaN point coordinates from ever hitting Mongo 2dsphere index
// 2) Save is resilient per alert (one bad doc won't block all others)
// 3) Wide jitter in miles for state-center fallback to reduce marker stacking
// 4) For CAP (FEMA/USGS), use county & geocode (FIPS/UGC) fallbacks instead of
//    always jumping straight to state-centroid jitter.

import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

// Optional: centers from FIPS/UGC codes for FEMA CAP
const fipsCentersPath = path.resolve(__dirname, "../data/fips_centers.json");
let fipsCenters = {};
try {
  if (fs.existsSync(fipsCentersPath)) {
    fipsCenters = JSON.parse(fs.readFileSync(fipsCentersPath, "utf8"));
  } else {
    console.warn(
      "ℹ️ fips_centers.json not found; CAP FIPS geocodes will fall back to counties/state."
    );
  }
} catch (err) {
  console.warn("⚠️ Failed to load fips_centers.json:", err.message);
  fipsCenters = {};
}

// UGC centroids intentionally not supported; modern CAP uses polygons/counties/FIPS
const ugcCenters = {};

/* ------------------------------------------------------------------ */
/*  CONFIG                                                            */
/* ------------------------------------------------------------------ */

// NOAA Weather API – active alerts for the whole US + territories
const NOAA_ALERTS_URL =
  process.env.NOAA_ALERTS_URL || "https://api.weather.gov/alerts/active";

// IMPORTANT: NOAA requires a User-Agent header.
const NOAA_USER_AGENT =
  process.env.NOAA_USER_AGENT ||
  "disaster-help-backend/1.0 (contact: change-me@example.com)";

// CAP-style XML feeds (FEMA, USGS). NWS CAP is intentionally not used.
const CAP_FEEDS = [
  {
    name: "FEMA IPAWS",
    url:
      process.env.FEMA_IPAWS_URL ||
      "https://ipaws.nws.noaa.gov/feeds/IPAWSOpenCAP.xml",
    source: "FEMA",
  },
  {
    name: "USGS Earthquakes",
    url:
      process.env.USGS_QUAKES_URL ||
      "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.atom",
    source: "USGS",
  },
];

/**
 * Anti-stacking jitter (deterministic per alert identifier).
 * Defaults are "wide" for state-centers to visibly spread stacked points.
 *
 * Env knobs:
 *  - ALERT_GEO_JITTER=false          disable all jitter
 *  - ALERT_JITTER_STATE_MIN_MILES=20
 *  - ALERT_JITTER_STATE_MAX_MILES=50
 *  - ALERT_JITTER_COUNTY_MIN_MILES=4
 *  - ALERT_JITTER_COUNTY_MAX_MILES=12
 */
const GEO_JITTER_ENABLED =
  String(process.env.ALERT_GEO_JITTER ?? "true").toLowerCase() !== "false";
const JITTER_STATE_MIN_MILES = safeNumber(
  process.env.ALERT_JITTER_STATE_MIN_MILES,
  20
);
const JITTER_STATE_MAX_MILES = safeNumber(
  process.env.ALERT_JITTER_STATE_MAX_MILES,
  50
);
const JITTER_COUNTY_MIN_MILES = safeNumber(
  process.env.ALERT_JITTER_COUNTY_MIN_MILES,
  4
);
const JITTER_COUNTY_MAX_MILES = safeNumber(
  process.env.ALERT_JITTER_COUNTY_MAX_MILES,
  12
);

/** Fallback centers for state-level alerts (lon, lat) */
const STATE_CENTERS = {
  FL: [-81.5158, 27.6648],
  TX: [-99.9018, 31.9686],
  CA: [-119.4179, 36.7783],
  NY: [-75.4999, 43.0003],
  NC: [-79.0193, 35.7596],
  VA: [-78.6569, 37.4316],
  GA: [-83.4412, 32.1656],
  AL: [-86.9023, 32.8067],
  OH: [-82.9071, 40.4173],
  PA: [-77.1945, 41.2033],
  MI: [-84.5361, 44.1822],
  LA: [-91.9623, 30.9843],
  IL: [-89.3985, 40.6331],
  IN: [-86.1349, 40.2672],
  SC: [-81.1637, 33.8361],
  KY: [-84.27, 37.8393],
  TN: [-86.5804, 35.5175],
  AR: [-92.3731, 34.9697],
  AZ: [-111.0937, 34.0489],
  CO: [-105.7821, 39.5501],
  WA: [-120.7401, 47.7511],
  OR: [-120.5542, 43.8041],
  NV: [-116.4194, 38.8026],
  OK: [-97.0929, 35.0078],
  MO: [-91.8318, 38.5739],
  WI: [-89.6165, 44.7863],
  MN: [-94.6859, 46.7296],
  IA: [-93.0977, 41.878],
  KS: [-98.4842, 39.0119],
  ME: [-69.4455, 45.2538],
  VT: [-72.5778, 44.5588],
  NH: [-71.5724, 43.1939],
  MA: [-71.3824, 42.4072],
  CT: [-72.6979, 41.6032],
  RI: [-71.4774, 41.5801],
  DE: [-75.5277, 38.9108],
  MD: [-76.6413, 39.0458],
  WV: [-80.4549, 38.5976],
  ND: [-100.5407, 47.5515],
  SD: [-99.9018, 43.9695],
  MT: [-110.3626, 46.8797],
  NE: [-99.9018, 41.4925],
  NM: [-105.8701, 34.5199],
  WY: [-107.2903, 43.0759],
  ID: [-114.742, 44.0682],
  UT: [-111.0937, 39.32],
  AK: [-152.4044, 64.2008],
  HI: [-155.5828, 19.8968],
  PR: [-66.5901, 18.2208],
  GU: [144.7937, 13.4443],
  VI: [-64.8963, 18.3358],
  DC: [-77.0369, 38.9072],
};

const STATE_NAME_TO_ABBR = {
  Alabama: "AL",
  Alaska: "AK",
  Arizona: "AZ",
  Arkansas: "AR",
  California: "CA",
  Colorado: "CO",
  Connecticut: "CT",
  Delaware: "DE",
  Florida: "FL",
  Georgia: "GA",
  Hawaii: "HI",
  Idaho: "ID",
  Illinois: "IL",
  Indiana: "IN",
  Iowa: "IA",
  Kansas: "KS",
  Kentucky: "KY",
  Louisiana: "LA",
  Maine: "ME",
  Maryland: "MD",
  Massachusetts: "MA",
  Michigan: "MI",
  Minnesota: "MN",
  Mississippi: "MS",
  Missouri: "MO",
  Montana: "MT",
  Nebraska: "NE",
  Nevada: "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  Ohio: "OH",
  Oklahoma: "OK",
  Oregon: "OR",
  Pennsylvania: "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  Tennessee: "TN",
  Texas: "TX",
  Utah: "UT",
  Vermont: "VT",
  Virginia: "VA",
  Washington: "WA",
  "West Virginia": "WV",
  Wisconsin: "WI",
  Wyoming: "WY",
  "District of Columbia": "DC",
  "Puerto Rico": "PR",
  Guam: "GU",
  "U.S. Virgin Islands": "VI",
  "US Virgin Islands": "VI",
  "American Samoa": "AS",
  "Northern Mariana Islands": "MP",
};

function safeNumber(v, fallback) {
  const n = typeof v === "string" && v.trim() !== "" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(n, lo, hi) {
  return Math.min(hi, Math.max(lo, n));
}

function wrapLon(lon) {
  if (!Number.isFinite(lon)) return lon;
  // wrap to [-180, 180]
  let x = lon;
  while (x > 180) x -= 360;
  while (x < -180) x += 360;
  return x;
}

function isFiniteLonLat(lon, lat) {
  return (
    Number.isFinite(lon) &&
    Number.isFinite(lat) &&
    Math.abs(lon) <= 180 &&
    Math.abs(lat) <= 90
  );
}

function sanitizePointGeometry(geom) {
  if (!geom || geom.type !== "Point" || !Array.isArray(geom.coordinates))
    return null;
  const [lon, lat] = geom.coordinates;
  if (!isFiniteLonLat(lon, lat)) return null;
  return { type: "Point", coordinates: [wrapLon(lon), lat] };
}

/* ------------------------------------------------------------------ */
/*  DETERMINISTIC JITTER (MILES)                                      */
/* ------------------------------------------------------------------ */

// FNV-1a 32-bit hash
function hash32(str) {
  const s = String(str ?? "");
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// Mulberry32 PRNG
function mulberry32(seed) {
  let a = seed >>> 0;
  return function rand() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Jitter a lon/lat point by an annulus (min..max) in miles
function jitterLonLatMiles([lon, lat], seedStr, minMiles, maxMiles) {
  if (!isFiniteLonLat(lon, lat)) return [lon, lat];

  const minM = Math.max(0, safeNumber(minMiles, 0));
  const maxM = Math.max(minM, safeNumber(maxMiles, minM));

  if (maxM === 0) return [lon, lat];

  const rand = mulberry32(hash32(seedStr));
  const u = rand();
  const v = rand();

  // Uniform by area in annulus
  const r = Math.sqrt(
    minM * minM + u * (maxM * maxM - minM * minM)
  ); // miles
  const theta = v * 2 * Math.PI;

  // Miles north/east
  const milesNorth = r * Math.cos(theta);
  const milesEast = r * Math.sin(theta);

  // Convert miles to degrees
  const latRad = (lat * Math.PI) / 180;
  const cosLat = Math.cos(latRad);
  const cosSafe = Math.max(0.15, Math.abs(cosLat)); // avoid blowups near poles

  const dLat = milesNorth / 69.0;
  const dLon = milesEast / (69.0 * cosSafe);

  const outLat = clamp(lat + dLat, -90, 90);
  const outLon = wrapLon(lon + dLon);

  if (!isFiniteLonLat(outLon, outLat)) return [lon, lat];
  return [outLon, outLat];
}

function maybeApplyJitter(pointGeom, geometryMethod, identifier) {
  if (!GEO_JITTER_ENABLED) return { geometry: pointGeom, geometryMethod };
  const g = sanitizePointGeometry(pointGeom);
  if (!g) return { geometry: null, geometryMethod };

  const method = String(geometryMethod || "").toLowerCase();

  // Only jitter fallback-ish methods; do not jitter true polygon/geo geometry.
  const isStateFallback =
    method.includes("state-center") || method.includes("states-centroid");
  const isCountyFallback =
    method.includes("county-fallback") ||
    method.includes("county-centroid") ||
    method.includes("county");

  if (!isStateFallback && !isCountyFallback) {
    return { geometry: g, geometryMethod };
  }

  const seedStr = `${identifier || "unknown"}|${geometryMethod || "unknown"}`;

  const [lon, lat] = g.coordinates;

  const [jLon, jLat] = isStateFallback
    ? jitterLonLatMiles(
        [lon, lat],
        seedStr,
        JITTER_STATE_MIN_MILES,
        JITTER_STATE_MAX_MILES
      )
    : jitterLonLatMiles(
        [lon, lat],
        seedStr,
        JITTER_COUNTY_MIN_MILES,
        JITTER_COUNTY_MAX_MILES
      );

  const out = { type: "Point", coordinates: [jLon, jLat] };
  const outSan = sanitizePointGeometry(out);

  if (!outSan) {
    // If anything went weird, fall back to original clean point.
    return { geometry: g, geometryMethod };
  }

  return {
    geometry: outSan,
    geometryMethod: `${geometryMethod}-jitter`,
  };
}

/* ------------------------------------------------------------------ */
/*  STATE / TEXT HELPERS                                              */
/* ------------------------------------------------------------------ */

function stateToAbbr(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (/^[A-Z]{2}$/.test(t)) return t;
  return STATE_NAME_TO_ABBR[t] || null;
}

function extractStateAbbrs(text) {
  if (!text) return [];
  const set = new Set();

  const codes =
    String(text).match(
      /\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR|GU|AS|MP|VI)\b/g
    ) || [];
  codes.forEach((c) => set.add(c));

  for (const name of Object.keys(STATE_NAME_TO_ABBR)) {
    const re = new RegExp(`\\b${name}\\b`, "i");
    if (re.test(text)) set.add(STATE_NAME_TO_ABBR[name]);
  }

  return Array.from(set);
}

/* ------------------------------------------------------------------ */
/*  GEOMETRY HELPERS                                                  */
/* ------------------------------------------------------------------ */

const medianAbs = (arr) => {
  const v = arr.map((x) => Math.abs(x)).sort((a, b) => a - b);
  const n = v.length;
  if (!n) return 0;
  return n % 2 ? v[(n - 1) / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
};

function detectLonLatOrder(pairs) {
  const aOutsideLat = pairs.reduce(
    (c, [a]) => c + (Math.abs(a) > 90 ? 1 : 0),
    0
  );
  const bOutsideLat = pairs.reduce(
    (c, [, b]) => c + (Math.abs(b) > 90 ? 1 : 0),
    0
  );
  if (aOutsideLat !== bOutsideLat)
    return aOutsideLat > bOutsideLat ? "lonlat" : "latlon";
  const aMed = medianAbs(pairs.map(([a]) => a));
  const bMed = medianAbs(pairs.map(([, b]) => b));
  return aMed - bMed > bMed - aMed ? "lonlat" : "latlon";
}

function parsePolygon(polygonString) {
  if (!polygonString) return null;
  try {
    let rawPairs = [];
    if (polygonString.includes(",")) {
      rawPairs = polygonString
        .trim()
        .split(/\s+/)
        .map((p) => {
          const [a, b] = p.split(",").map(Number);
          return [a, b];
        });
    } else {
      const nums = polygonString
        .trim()
        .split(/\s+/)
        .map(Number)
        .filter((x) => !isNaN(x));
      for (let i = 0; i + 1 < nums.length; i += 2)
        rawPairs.push([nums[i], nums[i + 1]]);
    }
    rawPairs = rawPairs.filter(([a, b]) => !isNaN(a) && !isNaN(b));
    if (rawPairs.length < 3) return null;
    const order = detectLonLatOrder(rawPairs);
    const coords = rawPairs.map(([a, b]) =>
      order === "latlon" ? [b, a] : [a, b]
    );
    const [firstLon, firstLat] = coords[0];
    const [lastLon, lastLat] = coords[coords.length - 1];
    if (firstLon !== lastLon || firstLat !== lastLat) coords.push(coords[0]);
    return { type: "Polygon", coordinates: [coords] };
  } catch (err) {
    console.warn("⚠️ Failed to parse polygon:", err.message);
    return null;
  }
}

function pointsCentroid(pts) {
  if (!pts || !pts.length) return null;
  let x = 0,
    y = 0,
    z = 0;

  for (const [lon, lat] of pts) {
    if (!isFiniteLonLat(lon, lat)) continue;
    const latR = (lat * Math.PI) / 180;
    const lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
  }

  const total = pts.length;
  if (!total) return null;

  x /= total;
  y /= total;
  z /= total;

  const lon = Math.atan2(y, x);
  const hyp = Math.sqrt(x * x + y * y);
  const lat = Math.atan2(z, hyp);

  const out = {
    type: "Point",
    coordinates: [lon * (180 / Math.PI), lat * (180 / Math.PI)],
  };
  return sanitizePointGeometry(out);
}

function polygonCentroid(geometry) {
  if (!geometry || geometry.type !== "Polygon") return null;
  return pointsCentroid(geometry.coordinates?.[0] || []);
}

function bboxFromPoints(points) {
  const pts = (points || []).filter(
    (p) =>
      Array.isArray(p) &&
      p.length >= 2 &&
      isFiniteLonLat(p[0], p[1])
  );
  if (!pts.length) return null;
  const lons = pts.map((p) => p[0]);
  const lats = pts.map((p) => p[1]);
  return [
    Math.min(...lons),
    Math.min(...lats),
    Math.max(...lons),
    Math.max(...lats),
  ];
}

function flattenNoaaGeometryPoints(geometry) {
  if (!geometry || !geometry.coordinates) return [];
  const { type, coordinates } = geometry;

  const fin2 = (p) =>
    Array.isArray(p) &&
    p.length >= 2 &&
    Number.isFinite(p[0]) &&
    Number.isFinite(p[1]);

  if (type === "Point") return fin2(coordinates) ? [coordinates] : [];

  if (type === "Polygon") return coordinates.flat().filter(fin2);

  if (type === "MultiPolygon") return coordinates.flat(2).filter(fin2);

  if (type === "LineString") return coordinates.filter(fin2);

  if (type === "MultiLineString") return coordinates.flat().filter(fin2);

  return [];
}

/* ------------------------------------------------------------------ */
/*  COUNTY / GEOCODE HELPERS                                          */
/* ------------------------------------------------------------------ */

function normalizeCountyName(raw) {
  if (!raw) return "";
  let s = String(raw).trim();
  s = s.replace(/^City of\s+/i, "");
  s = s.replace(
    /\s+(County|Parish|Borough|Census Area|Municipio|Municipality|City)$/i,
    ""
  );
  s = s.replace(/^St[.\s]+/i, "Saint ");
  s = s.replace(/\./g, "").replace(/\s+/g, " ").trim();
  return s;
}

function getCountyCenter(stateAbbr, countyRaw) {
  if (!stateAbbr || !countyRaw) return null;
  const stateMap = countyCenters[stateAbbr];
  if (!stateMap) return null;

  const norm = normalizeCountyName(countyRaw);

  if (stateMap[norm]) return stateMap[norm];

  const stripped = norm
    .replace(
      /^(Eastern|Western|Northern|Southern|Central|Coastal|Upper|Lower|Northeast|Northwest|Southeast|Southwest)\s+/i,
      ""
    )
    .trim();

  if (stripped && stateMap[stripped]) return stateMap[stripped];

  return null;
}

function collectCountyCentersFromAreaDesc(areaDesc, stateHint) {
  if (!areaDesc) return [];

  const regions = String(areaDesc)
    .split(/[;]+/) // NWS / CAP areaDesc is usually semicolon-separated
    .map((s) => s.trim())
    .filter(Boolean);

  const stateHints = stateHint ? [stateHint] : extractStateAbbrs(areaDesc);

  const pts = [];
  for (const r of regions) {
    // "Smith County, TX" or "Smith, Texas"
    let m = r.match(/^(.+?),\s*([A-Za-z .]+)$/);
    if (m) {
      const county = normalizeCountyName(m[1]);
      let abbr = stateToAbbr(m[2]);
      if (!abbr && stateHints.length === 1) abbr = stateHints[0];
      const p = getCountyCenter(abbr, county);
      if (p) pts.push(p);
      continue;
    }

    // County-only token if exactly one state hint
    if (stateHints.length === 1 && /^[A-Za-z .'-]+$/.test(r)) {
      const countyOnly = normalizeCountyName(r);
      const p = getCountyCenter(stateHints[0], countyOnly);
      if (p) pts.push(p);
    }
  }

  return pts;
}

/**
 * Exported helper retained for compatibility.
 * Returns centroid of all matched county centers when possible.
 */
function tryCountyCenterFromAreaDesc(areaDesc, stateHint) {
  const pts = collectCountyCentersFromAreaDesc(areaDesc, stateHint);
  if (!pts.length) return null;
  if (pts.length === 1) return { type: "Point", coordinates: pts[0] };
  const centroid = pointsCentroid(pts);
  return centroid;
}

/**
 * Centers from FIPS / UGC codes (FEMA CAP geocodes).
 * Uses fips_centers.json and ugc_centers.json if present.
 */
function centersFromFipsAndUgc(fipsCodes = [], ugcCodes = []) {
  // UGC intentionally ignored (Option A).
  const pts = [];

  for (const code of fipsCodes) {
    const p = fipsCenters?.[code];
    if (Array.isArray(p) && p.length >= 2 && isFiniteLonLat(p[0], p[1])) {
      pts.push(p);
    }
  }

  return pts;
}

/* ------------------------------------------------------------------ */
/*  DATE HELPERS                                                      */
/* ------------------------------------------------------------------ */

function parseDateMaybe(v) {
  if (!v) return new Date();
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

/* ------------------------------------------------------------------ */
/*  CAP-STYLE NORMALIZATION (FEMA / USGS)                             */
/* ------------------------------------------------------------------ */

function normalizeCapAlert(entry, feed) {
  try {
    const source = feed.source;

    const root =
      entry.alert ||
      entry["cap:alert"] ||
      entry.content?.alert ||
      entry.content?.["cap:alert"] ||
      entry;

    const infoRaw = root.info || root["cap:info"] || {};
    const info = Array.isArray(infoRaw) ? infoRaw[0] : infoRaw || {};

    const areaRaw = info.area || info["cap:area"] || {};
    const area = Array.isArray(areaRaw) ? areaRaw[0] : areaRaw || {};

    const areaDesc =
      area.areaDesc ||
      area["cap:areaDesc"] ||
      info.areaDesc ||
      info["cap:areaDesc"] ||
      root.areaDesc ||
      root["cap:areaDesc"] ||
      "";

    // CAP geocodes (FIPS / UGC / SAME etc.)
    const geocodeRaw =
      area.geocode ||
      area["cap:geocode"] ||
      info.geocode ||
      info["cap:geocode"] ||
      [];
    const geocodes = Array.isArray(geocodeRaw) ? geocodeRaw : [geocodeRaw];

    const fipsCodes = [];
    const ugcCodes = [];
    for (const gc of geocodes) {
      if (!gc) continue;
      const valueName =
        gc.valueName || gc["cap:valueName"] || gc["@_valueName"] || "";
      const value =
        gc.value || gc["cap:value"] || gc["@_value"] || "";
      if (!valueName || !value) continue;

      const codes = String(value).trim().split(/\s+/);
      if (/FIPS/i.test(valueName)) fipsCodes.push(...codes);
    }

    let polygonRaw =
      area.polygon ||
      area["cap:polygon"] ||
      info.polygon ||
      info["cap:polygon"] ||
      root.polygon ||
      root["cap:polygon"] ||
      null;

    if (Array.isArray(polygonRaw)) polygonRaw = polygonRaw.join(" ");
    const polygonGeom = parsePolygon(polygonRaw);

    let geometry = null;
    let geometryMethod = null;

    // 1) Polygon centroid
    if (polygonGeom) {
      geometry = polygonCentroid(polygonGeom);
      geometryMethod = "polygon";
    }

    // 2) georss:point (USGS Atom uses this often)
    if (!geometry) {
      const pointStr =
        root.point ||
        root["georss:point"] ||
        info.point ||
        info["georss:point"];
      if (typeof pointStr === "string") {
        const parts = pointStr
          .trim()
          .split(/\s+/)
          .map(Number);
        if (
          parts.length >= 2 &&
          Number.isFinite(parts[0]) &&
          Number.isFinite(parts[1])
        ) {
          const [lat, lon] = parts;
          geometry = sanitizePointGeometry({
            type: "Point",
            coordinates: [lon, lat],
          });
          geometryMethod = "georss-point";
        }
      }
    }

    // 3) explicit lat/lon fields
    if (!geometry) {
      const lat = Number(info.lat || area.lat || root.lat);
      const lon = Number(info.lon || area.lon || root.lon);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        geometry = sanitizePointGeometry({
          type: "Point",
          coordinates: [lon, lat],
        });
        geometryMethod = "explicit-latlon";
      }
    }

   // 3.0) geocode-based centers (FIPS only)
   if (!geometry && fipsCodes.length) {
     const geoPts = centersFromFips(fipsCodes);
      if (geoPts.length === 1) {
        geometry = sanitizePointGeometry({
          type: "Point",
          coordinates: geoPts[0],
        });
        geometryMethod = "cap-geocode-centroid";
      } else if (geoPts.length > 1) {
        geometry = pointsCentroid(geoPts);
        geometryMethod = "cap-geocode-centroid";
      }
    }

    // 3.5) county-centroid fallback from areaDesc (before state-center)
    if (!geometry && areaDesc) {
      const stateList = extractStateAbbrs(areaDesc);
      const stateHint = stateList.length === 1 ? stateList[0] : null;
      const countyGeom = tryCountyCenterFromAreaDesc(areaDesc, stateHint);
      if (countyGeom) {
        geometry = countyGeom;
        geometryMethod = "cap-county-centroid";
      }
    }

    // 4) very last resort: try to infer state(s) from text and use centers centroid
    if (!geometry && areaDesc) {
      const stateList = extractStateAbbrs(areaDesc);
      const pts = stateList.map((s) => STATE_CENTERS[s]).filter(Boolean);
      if (pts.length === 1) {
        geometry = sanitizePointGeometry({
          type: "Point",
          coordinates: pts[0],
        });
        geometryMethod = "state-center";
      } else if (pts.length > 1) {
        geometry = pointsCentroid(pts);
        geometryMethod = "states-centroid";
      }
    }

    const identifier = String(
      root.identifier ||
        root.id ||
        `CAP-${Date.now()}-${Math.random()
          .toString(36)
          .slice(2, 8)}`
    );

    // Apply jitter ONLY if this is a fallback method (state/county), not polygon/true point
    if (geometry && geometryMethod) {
      const jittered = maybeApplyJitter(
        geometry,
        geometryMethod,
        identifier
      );
      geometry = jittered.geometry;
      geometryMethod = jittered.geometryMethod;
    }

    if (!geometry) {
      console.warn(
        `🚫 Skipping CAP alert with no usable geometry: ${
          areaDesc || "(no areaDesc)"
        }`
      );
      return null;
    }

    let bbox = null;
    if (polygonGeom?.coordinates?.[0]?.length > 2) {
      bbox = bboxFromPoints(polygonGeom.coordinates[0]);
    }

    let expires = null;
    const expiresRaw = info.expires || root.expires || null;
    if (expiresRaw) {
      const parsed = new Date(expiresRaw);
      expires = isNaN(parsed.getTime())
        ? new Date(Date.now() + 60 * 60 * 1000)
        : parsed;
    } else {
      expires = new Date(Date.now() + 60 * 60 * 1000);
    }

    let eventName =
      info.event ||
      root.event ||
      (root.title && String(root.title).split(" issued")[0]) ||
      (root.summary && String(root.summary).split(" issued")[0]) ||
      "Alert";

    // USGS: shorten TTL & label
    if (source === "USGS") {
      const magMatch = String(root?.title || "").match(/M\s?(\d+\.\d+)/);
      const magnitude = magMatch ? parseFloat(magMatch[1]) : null;
      const sentTime = parseDateMaybe(
        info.effective || root.sent || root.updated || Date.now()
      );

      if (magnitude !== null && Number.isFinite(magnitude)) {
        if (magnitude < 3.0) {
          eventName = `Seismic Activity (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 10 * 60 * 1000);
        } else if (magnitude < 5.0) {
          eventName = `Minor Earthquake (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 60 * 60 * 1000);
        } else {
          eventName = `Earthquake (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 3 * 60 * 60 * 1000);
        }
      } else {
        eventName = "Seismic Activity";
        expires = new Date(sentTime.getTime() + 10 * 60 * 1000);
      }
    }

    const headlineText =
      info.headline || root.title || root.summary || eventName;

    let descriptionText =
      info.description || root.summary || root.content || "";
    if (
      typeof descriptionText === "object" &&
      descriptionText["#text"]
    )
      descriptionText = descriptionText["#text"];
    descriptionText = String(descriptionText ?? "")
      .replace(/<dt>/g, "\n")
      .replace(/<\/dt>/g, ": ")
      .replace(/<dd>/g, "")
      .replace(/<\/dd>/g, "")
      .replace(/<\/?dl>/g, "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]*>/g, "")
      .replace(/&deg;/g, "°")
      .replace(/[ \t]+/g, " ")
      .replace(/\n\s*\n/g, "\n")
      .trim();

    let instructionText = info.instruction || root.instruction || "";
    instructionText = String(instructionText ?? "").trim();

    const infoBlock = {
      category: info.category || "General",
      event: String(eventName).trim(),
      urgency: info.urgency || (source === "USGS" ? "Past" : "Unknown"),
      severity:
        info.severity || (source === "USGS" ? "Minor" : "Unknown"),
      certainty:
        info.certainty || (source === "USGS" ? "Observed" : "Unknown"),
      headline: String(headlineText).trim(),
      description: String(descriptionText).trim(),
      instruction: String(instructionText).trim(),
    };

    return {
      identifier,
      sender: String(root.sender || ""),
      sent: parseDateMaybe(
        info.effective ||
          root.sent ||
          root.updated ||
          root.published ||
          new Date()
      ),
      status: root.status || "Actual",
      msgType: root.msgType || "Alert",
      scope: root.scope || "Public",
      info: infoBlock,
      area: { areaDesc: areaDesc || "", polygon: polygonRaw || null },
      geometry,
      geometryMethod,
      bbox,
      hasGeometry: true,
      title: String(headlineText).trim(),
      summary: String(descriptionText).trim(),
      source,
      timestamp: new Date(),
      expires,
    };
  } catch (err) {
    console.error("❌ Error normalizing CAP alert:", err.message);
    return null;
  }
}

/* ------------------------------------------------------------------ */
/*  NOAA WEATHER API NORMALIZATION (NWS ALERTS)                       */
/* ------------------------------------------------------------------ */

function inferStateAbbrsFromNoaa(props = {}) {
  const set = new Set();

  const geocode = props.geocode || {};
  const zones = [];

  if (Array.isArray(geocode.UGC)) zones.push(...geocode.UGC);
  if (Array.isArray(geocode.ugc)) zones.push(...geocode.ugc);

  if (Array.isArray(props.affectedZones)) {
    for (const z of props.affectedZones) {
      const code = String(z).split("/").pop(); // e.g. COZ012 or CAC001
      if (code) zones.push(code);
    }
  }

  for (const zone of zones) {
    if (typeof zone !== "string" || zone.length < 2) continue;
    const abbr = zone.slice(0, 2).toUpperCase();
    if (STATE_CENTERS[abbr]) set.add(abbr);
  }

  // SenderName often ends with state code: "NWS Pueblo CO"
  const senderName = String(props.senderName || props.sender || "");
  const m = senderName.match(/\b([A-Z]{2})\b\s*$/);
  if (m && STATE_CENTERS[m[1]]) set.add(m[1]);

  // Text fallback
  const areaDesc = props.areaDesc || "";
  for (const abbr of extractStateAbbrs(areaDesc)) set.add(abbr);

  return Array.from(set);
}

function normalizeNoaaAlert(feature) {
  try {
    if (!feature || typeof feature !== "object") return null;

    const props = feature.properties || {};
    const areaDesc = props.areaDesc || "";

    // --- try NOAA GeoJSON geometry (preferred, accurate)
    let geometry = null;
    let geometryMethod = null;
    let bbox = null;

    const pts = flattenNoaaGeometryPoints(feature.geometry);
    if (pts.length) {
      geometry = pointsCentroid(pts);
      bbox = bboxFromPoints(pts);
      geometryMethod = `noaa-geom-${String(
        feature.geometry.type || "geom"
      ).toLowerCase()}`;
    }

    // --- county centroid fallback (legacy-ish)
    if (!geometry && areaDesc) {
      const states = inferStateAbbrsFromNoaa(props);
      const stateHint = states.length === 1 ? states[0] : null;
      const countyPts = collectCountyCentersFromAreaDesc(
        areaDesc,
        stateHint
      );
      if (countyPts.length === 1) {
        geometry = sanitizePointGeometry({
          type: "Point",
          coordinates: countyPts[0],
        });
        geometryMethod = "noaa-county-centroid";
      } else if (countyPts.length > 1) {
        geometry = pointsCentroid(countyPts);
        geometryMethod = "noaa-county-centroid";
      }
    }

    // --- state centroid fallback (multi-state supported)
    if (!geometry) {
      const states = inferStateAbbrsFromNoaa(props);
      const centers = states.map((s) => STATE_CENTERS[s]).filter(Boolean);

      if (centers.length === 1) {
        geometry = sanitizePointGeometry({
          type: "Point",
          coordinates: centers[0],
        });
        geometryMethod = "noaa-state-center";
      } else if (centers.length > 1) {
        geometry = pointsCentroid(centers);
        geometryMethod = "noaa-states-centroid";
      }
    }

    if (!geometry) return null;

    const identifier =
      String(props.id || "") ||
      (typeof props["@id"] === "string" &&
      props["@id"].includes("/")
        ? props["@id"].split("/").pop()
        : props["@id"]) ||
      (typeof feature.id === "string" && feature.id.includes("/")
        ? feature.id.split("/").pop()
        : feature.id) ||
      `NOAA-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2, 8)}`;

    // Apply jitter only to fallback geometry (state/county), not to polygon-derived geometry
    if (geometry && geometryMethod) {
      const jittered = maybeApplyJitter(
        geometry,
        geometryMethod,
        identifier
      );
      geometry = jittered.geometry;
      geometryMethod = jittered.geometryMethod;
    }

    // Final validation (never allow NaN into Mongo)
    geometry = sanitizePointGeometry(geometry);
    if (!geometry) return null;

    const sent = parseDateMaybe(
      props.sent ||
        props.effective ||
        props.onset ||
        props.updated ||
        new Date()
    );

    let expires = null;
    if (props.expires) {
      const parsed = new Date(props.expires);
      expires = isNaN(parsed.getTime())
        ? new Date(Date.now() + 60 * 60 * 1000)
        : parsed;
    } else {
      expires = new Date(Date.now() + 60 * 60 * 1000);
    }

    const eventName =
      props.event ||
      (props.headline &&
        String(props.headline).split(" issued")[0]) ||
      "Alert";
    const headlineText = props.headline || eventName;

    let descriptionText = String(props.description || "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]*>/g, "")
      .replace(/&deg;/g, "°")
      .replace(/[ \t]+/g, " ")
      .replace(/\n\s*\n/g, "\n")
      .trim();

    let instructionText = String(props.instruction || "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]*>/g, "")
      .replace(/[ \t]+/g, " ")
      .replace(/\n\s*\n/g, "\n")
      .trim();

    const infoBlock = {
      category: props.category || "Met",
      event: String(eventName).trim(),
      urgency: props.urgency || "Unknown",
      severity: props.severity || "Unknown",
      certainty: props.certainty || "Unknown",
      headline: String(headlineText).trim(),
      description: descriptionText.trim(),
      instruction: instructionText.trim(),
    };

    return {
      identifier,
      sender: props.senderName || "NWS",
      sent,
      status: props.status || "Actual",
      msgType: props.messageType || props.message_type || "Alert",
      scope: props.scope || "Public",
      info: infoBlock,
      area: { areaDesc: areaDesc || "", polygon: null },
      geometry,
      geometryMethod,
      bbox,
      hasGeometry: true,
      title: String(headlineText).trim(),
      summary: descriptionText.trim(),
      source: "NWS",
      timestamp: new Date(),
      expires,
    };
  } catch (err) {
    console.error("❌ Error normalizing NOAA alert:", err.message);
    return null;
  }
}

/* ------------------------------------------------------------------ */
/*  DB SAVE                                                           */
/* ------------------------------------------------------------------ */

async function saveAlerts(alerts) {
  if (!alerts || !alerts.length) return;

  const db = getDB();
  const collection = db.collection("alerts_cap");

  // Safety net cleanup (works only if sent is a Date; we store it as Date now)
  const cutoff = new Date(Date.now() - 72 * 60 * 60 * 1000);
  try {
    await collection.deleteMany({ sent: { $lt: cutoff } });
  } catch (e) {
    // ignore, TTL index is the real cleanup
  }

  let saved = 0;
  let skipped = 0;

  for (const alert of alerts) {
    try {
      const geom = sanitizePointGeometry(alert.geometry);
      if (!geom) {
        skipped++;
        continue;
      }

      const doc = {
        ...alert,
        geometry: geom,
      };

      // Ensure bbox is sane (optional)
      if (Array.isArray(doc.bbox) && doc.bbox.length === 4) {
        const ok = doc.bbox.every((n) => Number.isFinite(n));
        if (!ok) doc.bbox = null;
      }

      await collection.updateOne(
        { identifier: alert.identifier },
        { $set: doc },
        { upsert: true }
      );

      saved++;
    } catch (err) {
      // This is the critical change: don't let one bad alert block all others.
      skipped++;
      console.warn(
        "⚠️ Skipped alert during save:",
        alert?.identifier,
        "-",
        err.message
      );
    }
  }

  console.log(
    `💾 Saved ${saved} alerts to MongoDB (skipped ${skipped})`
  );
}

/* ------------------------------------------------------------------ */
/*  FETCHERS                                                          */
/* ------------------------------------------------------------------ */

async function fetchCapFeed(feed) {
  console.log(`🌐 Fetching ${feed.name} (${feed.source})`);
  try {
    const res = await axios.get(feed.url, { timeout: 20000 });
    const xml = res.data;

    const parser = new XMLParser({
      ignoreAttributes: false,
      removeNSPrefix: true,
      attributeNamePrefix: "@_",
      trimValues: true,
      isArray: (tagName) =>
        tagName === "entry" ||
        tagName === "info" ||
        tagName === "area" ||
        tagName === "geocode", // 👈 ensure geocode blocks come through as arrays
    });

    const json = parser.parse(xml);

    let entries = json.alert ? [json.alert] : json.feed?.entry || [];
    if (!Array.isArray(entries)) entries = [entries];

    const alerts = entries
      .map((e) => normalizeCapAlert(e, feed))
      .filter(Boolean);
    const usable = alerts.filter((a) => a.hasGeometry).length;

    console.log(
      `✅ Parsed ${alerts.length} alerts from ${feed.source} (${usable} usable geo)`
    );
    if (alerts.length) await saveAlerts(alerts);
  } catch (err) {
    console.error(`❌ Error fetching ${feed.name}:`, err.message);
  }
}

async function fetchNoaaAlerts() {
  console.log("🌐 Fetching NOAA NWS Alerts (api.weather.gov)...");
  try {
    const res = await axios.get(NOAA_ALERTS_URL, {
      timeout: 20000,
      headers: {
        "User-Agent": NOAA_USER_AGENT,
        Accept: "application/geo+json",
      },
    });

    const data = res.data || {};
    const features = Array.isArray(data.features) ? data.features : [];

    const alerts = [];
    let skipped = 0;

    for (const feature of features) {
      const alert = normalizeNoaaAlert(feature);
      if (alert) alerts.push(alert);
      else skipped++;
    }

    console.log(
      `✅ NOAA parsed ${alerts.length} alerts (${features.length} raw, ${skipped} skipped)`
    );

    if (alerts.length) await saveAlerts(alerts);
  } catch (err) {
    console.error("❌ Error fetching NOAA alerts:", err.message);
  }
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

async function pollCapFeeds() {
  console.log("🚨 CAP/NOAA Poller running...");

  // TTL cleanup fallback (expires is the key for TTL)
  try {
    const db = getDB();
    const collection = db.collection("alerts_cap");
    const now = new Date();

    const { deletedCount } = await collection.deleteMany({
      $or: [
        { expires: { $lte: now } },
        { expires: { $exists: false } },
        { expires: null },
      ],
    });

    if (deletedCount > 0) {
      console.log(`🧹 Cleaned up ${deletedCount} expired CAP/NOAA alerts`);
    }
  } catch (err) {
    console.error("⚠️ TTL cleanup failed:", err.message);
  }

  // 1) NOAA Weather API (primary official NWS source)
  await fetchNoaaAlerts();

  // 2) FEMA + USGS
  for (const feed of CAP_FEEDS) {
    await fetchCapFeed(feed);
  }

  console.log("✅ CAP/NOAA poll cycle complete.\n");
}

export { pollCapFeeds, tryCountyCenterFromAreaDesc };
