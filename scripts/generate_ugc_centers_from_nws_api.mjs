// scripts/generate_ugc_centers_from_nws_api.mjs
//
// SAFE version: Fetches list of all forecast zones (UGC codes),
// then retrieves geometry for EACH zone individually.
//
// This avoids huge multi-geometry payloads and works reliably on Render.

import fs from "fs";
import path from "path";
import axios from "axios";

const USER_AGENT =
  process.env.NOAA_USER_AGENT ||
  "disaster-help-backend/1.0 (contact: you@example.com)";

const BASE = "https://api.weather.gov";
const OUT_PATH = path.resolve("src/data/ugc_centers.json");

// ---- helper: flatten geometry ----
function flatten(geom) {
  if (!geom || !geom.coordinates) return [];
  const t = geom.type;
  const c = geom.coordinates;

  const isPoint = (p) =>
    Array.isArray(p) &&
    p.length >= 2 &&
    Number.isFinite(p[0]) &&
    Number.isFinite(p[1]);

  if (t === "Point") return isPoint(c) ? [c] : [];
  if (t === "Polygon") return c.flat().filter(isPoint);
  if (t === "MultiPolygon") return c.flat(2).filter(isPoint);
  return [];
}

// ---- helper: centroid ----
function centroid(points) {
  if (!points.length) return null;
  let x = 0,
    y = 0,
    z = 0;
  let n = 0;
  for (const [lon, lat] of points) {
    const latR = (lat * Math.PI) / 180;
    const lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
    n++;
  }
  x /= n;
  y /= n;
  z /= n;

  const lon = Math.atan2(y, x) * (180 / Math.PI);
  const hyp = Math.sqrt(x * x + y * y);
  const lat = Math.atan2(z, hyp) * (180 / Math.PI);

  return [lon, lat];
}

// ---- main ----

async function fetchList() {
  const url = `${BASE}/zones/forecast?limit=1000`;
  const res = await axios.get(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "application/geo+json" },
  });
  return res.data.features.map((f) => f.properties.id);
}

async function fetchGeometry(id) {
  const url = `${BASE}/zones/forecast/${id}`;
  const res = await axios.get(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "application/geo+json" },
  });
  return res.data.geometry;
}

async function main() {
  console.log("📥 Fetching UGC (forecast) zone list...");
  const ids = await fetchList();
  console.log(`📌 ${ids.length} forecast zones found.`);

  const out = {};

  let count = 0;
  for (const id of ids) {
    try {
      const geom = await fetchGeometry(id);
      const pts = flatten(geom);
      const c = centroid(pts);
      if (c) out[id] = c;
      count++;
      if (count % 50 === 0) console.log(`   processed ${count}`);
    } catch (e) {
      console.log(`   ⚠️ failed ${id}: ${e.message}`);
    }
  }

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(out));
  console.log(`\n✅ Done. Wrote ${Object.keys(out).length} UGC centroids → ${OUT_PATH}`);
}

main().catch((err) => {
  console.error("❌ ERROR:", err.message);
  process.exit(1);
});
