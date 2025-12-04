// scripts/generate_ugc_centers_from_nws_api.mjs
//
// Generates src/data/ugc_centers.json by pulling zone geometries from api.weather.gov
// and computing a stable centroid per zone id (e.g., COZ012, CAC001, GMZ530).
//
// Uses the official endpoint: https://api.weather.gov/zones/:type?include_geometry=true&limit=...
// Params shown in the zones docs.
//
// Run (PowerShell):
//   node scripts/generate_ugc_centers_from_nws_api.mjs

import axios from "axios";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://api.weather.gov";
const OUT_PATH = path.resolve(__dirname, "../src/data/ugc_centers.json");

// These cover most UGCs you’ll see in CAP/NWS land + marine.
// If you want “minimum viable”, start with ["forecast","county"].
const ZONE_TYPES = ["forecast", "county", "fire", "marine"];

// NWS encourages a descriptive User-Agent; your backend already uses NOAA_USER_AGENT.
const USER_AGENT =
  process.env.NOAA_USER_AGENT ||
  process.env.NWS_USER_AGENT ||
  "disaster-help-backend/1.0 (contact: you@example.com)";

const LIMIT = 500;

// ---------- geo helpers ----------
function clamp(n, lo, hi) {
  return Math.min(hi, Math.max(lo, n));
}
function wrapLon(lon) {
  if (!Number.isFinite(lon)) return lon;
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

function flattenGeometryPoints(geometry) {
  if (!geometry || !geometry.coordinates) return [];
  const { type, coordinates } = geometry;

  const fin2 = (p) =>
    Array.isArray(p) &&
    p.length >= 2 &&
    Number.isFinite(p[0]) &&
    Number.isFinite(p[1]) &&
    isFiniteLonLat(p[0], p[1]);

  if (type === "Point") return fin2(coordinates) ? [coordinates] : [];
  if (type === "Polygon") return coordinates.flat().filter(fin2);
  if (type === "MultiPolygon") return coordinates.flat(2).filter(fin2);
  if (type === "LineString") return coordinates.filter(fin2);
  if (type === "MultiLineString") return coordinates.flat().filter(fin2);

  return [];
}

// Spherical mean of lon/lat points (stable + good enough for plotting)
function pointsCentroid(pts) {
  if (!pts || !pts.length) return null;
  let x = 0, y = 0, z = 0;
  let n = 0;

  for (const [lon, lat] of pts) {
    if (!isFiniteLonLat(lon, lat)) continue;
    const latR = (lat * Math.PI) / 180;
    const lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
    n++;
  }

  if (!n) return null;

  x /= n; y /= n; z /= n;

  const outLon = Math.atan2(y, x) * (180 / Math.PI);
  const hyp = Math.sqrt(x * x + y * y);
  const outLat = Math.atan2(z, hyp) * (180 / Math.PI);

  const lon = wrapLon(outLon);
  const lat = clamp(outLat, -90, 90);

  return isFiniteLonLat(lon, lat) ? [lon, lat] : null;
}

// ---------- http helpers ----------
async function fetchJson(url) {
  const res = await axios.get(url, {
    timeout: 30000,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/geo+json",
    },
  });
  return res.data;
}

function normalizeNext(next) {
  if (!next) return null;
  const s = String(next);
  if (s.startsWith("http")) return s;
  if (s.startsWith("/")) return `${BASE}${s}`;
  try {
    return new URL(s, BASE).toString();
  } catch {
    return null;
  }
}

function pickNextUrl(data) {
  // NWS APIs often include a pagination block; we accept multiple common shapes.
  const p = data?.pagination;
  if (typeof p?.next === "string") return p.next;
  if (typeof p?.next?.href === "string") return p.next.href;

  // Sometimes links are arrays with rel/ href
  const links = p?.links || data?.links;
  if (Array.isArray(links)) {
    const nxt = links.find((l) => (l?.rel || l?.relation || "").toLowerCase() === "next");
    if (typeof nxt?.href === "string") return nxt.href;
    if (typeof nxt?.url === "string") return nxt.url;
  }
  return null;
}

function zoneIdFromFeature(feature) {
  const raw =
    feature?.properties?.id ||
    feature?.id ||
    feature?.properties?.["@id"] ||
    feature?.properties?.zoneId ||
    "";
  const last = String(raw).split("/").pop().trim();
  return last ? last.toUpperCase() : null;
}

// ---------- main ----------
async function ingestZoneType(type, outMap) {
  console.log(`\n🌐 Pulling zones (${type}) from api.weather.gov ...`);

  let url = `${BASE}/zones/${type}?include_geometry=true&limit=${LIMIT}`; // supports include_geometry/limit
  let pages = 0;
  let added = 0;
  const seenNext = new Set();

  while (url) {
    pages++;
    const data = await fetchJson(url);
    const features = Array.isArray(data?.features) ? data.features : [];

    for (const f of features) {
      const zid = zoneIdFromFeature(f);
      if (!zid) continue;

      // Compute from this page geometry (include_geometry=true)
      let pts = flattenGeometryPoints(f.geometry);
      let c = pointsCentroid(pts);

      // If geometry is missing for any reason, fetch the zone detail as a fallback.
      if (!c) {
        try {
          const detail = await fetchJson(`${BASE}/zones/${type}/${zid}`);
          pts = flattenGeometryPoints(detail?.geometry);
          c = pointsCentroid(pts);
        } catch {
          // ignore
        }
      }

      if (!c) continue;

      // Key by UGC-ish zone id (COZ012, CAC001, GMZ530, etc)
      outMap[zid] = c;
      added++;
    }

    const next = normalizeNext(pickNextUrl(data));

    if (!next || seenNext.has(next)) break;
    seenNext.add(next);
    url = next;
  }

  console.log(`✅ ${type}: processed ${pages} page(s), wrote/updated ${added} centroids.`);
}

async function main() {
  const outMap = {};

  for (const t of ZONE_TYPES) {
    await ingestZoneType(t, outMap);
  }

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(outMap, null, 2), "utf8");

  console.log(`\n✅ DONE: wrote ${Object.keys(outMap).length} total entries to:`);
  console.log(`   ${OUT_PATH}`);
}

main().catch((err) => {
  console.error("❌ Failed to generate UGC centers:", err?.message || err);
  process.exit(1);
});
