// scripts/generate_ugc_centers_from_nws_api.mjs
//
// FINAL RENDER-SAFE VERSION
// Fetches ~1000 forecast UGC zones from api.weather.gov one-by-one,
// computes centroids, and writes them to src/data/ugc_centers.json.
//
// This version:
//  ✔ Follows redirects
//  ✔ Handles HTML error responses
//  ✔ Retries failed zone fetches
//  ✔ Logs detailed failures
//  ✔ Writes incremental progress every 50 zones
//  ✔ Uses correct User-Agent and Accept headers
//  ✔ Works reliably in Render shell
//
// Run in Render shell:
//    cd /opt/render/project/src
//    node scripts/generate_ugc_centers_from_nws_api.mjs

import fs from "fs";
import axios from "axios";
import path from "path";

// NOAA requires a real User-Agent (your backend already uses this)
const USER_AGENT =
  process.env.NOAA_USER_AGENT ||
  "DisasterHelpBackend/1.0 (contact: you@example.com)";

// Output file path
const OUT_PATH = path.resolve("src/data/ugc_centers.json");

// ------------------------------------
// Geometry helpers
// ------------------------------------
function flattenGeometry(geom) {
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

// Spherical centroid of lon/lat points
function centroid(points) {
  if (!points.length) return null;

  let x = 0, y = 0, z = 0;

  for (const [lon, lat] of points) {
    const latR = (lat * Math.PI) / 180;
    const lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
  }

  x /= points.length;
  y /= points.length;
  z /= points.length;

  const lon = Math.atan2(y, x) * (180 / Math.PI);
  const hyp = Math.sqrt(x * x + y * y);
  const lat = Math.atan2(z, hyp) * (180 / Math.PI);

  return [lon, lat];
}

// ------------------------------------
// Robust fetch helpers
// ------------------------------------
async function safeGetJSON(url, retries = 4) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await axios.get(url, {
        timeout: 20000,
        maxRedirects: 5,              // follow redirects
        validateStatus: () => true,   // allow non-200
        responseType: "json",
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "application/geo+json, application/json",
          "Accept-Encoding": "gzip"
        }
      });

      // If NOAA returns HTML, skip
      const ct = res.headers["content-type"] || "";
      if (!ct.includes("json")) {
        throw new Error(`Non-JSON content-type: ${ct}`);
      }

      if (res.status >= 200 && res.status < 300) {
        return res.data;
      }

      throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise((r) => setTimeout(r, 500));
    }
  }
}

// ------------------------------------
// Fetch UGC zone list
// ------------------------------------
async function fetchZoneList() {
  const url = "https://api.weather.gov/zones/forecast?limit=1500";
  const data = await safeGetJSON(url);

  if (!data || !Array.isArray(data.features)) {
    throw new Error("Invalid zone list response");
  }

  return data.features.map((f) => f.properties.id);
}

// Fetch geometry for one UGC zone
async function fetchZoneGeometry(ugc) {
  const url = `https://api.weather.gov/zones/forecast/${ugc}`;
  const data = await safeGetJSON(url);
  return data.geometry;
}

// ------------------------------------
// MAIN
// ------------------------------------
async function main() {
  console.log("📥 Fetching UGC forecast zone list...");

  const ugcs = await fetchZoneList();
  console.log(`📌 Found ${ugcs.length} forecast zones.`);

  const output = {};
  let processed = 0;

  for (const ugc of ugcs) {
    processed++;

    try {
      const geom = await fetchZoneGeometry(ugc);
      const pts = flattenGeometry(geom);
      const c = centroid(pts);

      if (c) {
        output[ugc] = c;
      } else {
        console.log(`⚠️  No centroid for ${ugc} (no valid points)`);
      }

    } catch (err) {
      console.log(`❌  Failed ${ugc}: ${err.message}`);
    }

    // Every 50 zones, write a progress snapshot
    if (processed % 50 === 0) {
      console.log(`   processed ${processed}/${ugcs.length}`);
      fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
      fs.writeFileSync(OUT_PATH, JSON.stringify(output));
    }
  }

  // Final write
  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(output));

  console.log(`\n✅ DONE — wrote ${Object.keys(output).length} UGC centroids → ${OUT_PATH}`);
}

main().catch((err) => {
  console.error("❌ FATAL:", err.message);
  process.exit(1);
});
