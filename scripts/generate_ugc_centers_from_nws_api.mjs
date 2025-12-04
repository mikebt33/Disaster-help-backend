// FINAL UGC GENERATOR — FAST, RELIABLE, NO NOAA THROTTLING
// Uses NOAA's built-in centroid data instead of full geometries.
// This runs instantly and works perfectly on Render.

import fs from "fs";
import axios from "axios";
import path from "path";

const USER_AGENT =
  process.env.NOAA_USER_AGENT ||
  "DisasterHelpBackend/1.0 (contact: you@example.com)";

const OUT_PATH = path.resolve("src/data/ugc_centers.json");

async function main() {
  console.log("📥 Fetching forecast zones with built-in centroids...");

  const url = "https://api.weather.gov/zones/forecast?limit=2000";

  const res = await axios.get(url, {
    timeout: 30000,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/geo+json"
    }
  });

  const features = res.data.features || [];
  console.log(`📌 Received ${features.length} forecast zones`);

  const out = {};

  for (const f of features) {
    const ugc = f.properties?.id;
    const cent = f.properties?.centroid || f.properties?.centroids;

    if (!ugc || !cent) continue;

    const lon = Number(cent.longitude);
    const lat = Number(cent.latitude);

    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    out[ugc] = [lon, lat];
  }

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(out));

  console.log(`\n✅ DONE — wrote ${Object.keys(out).length} UGC centroids → ${OUT_PATH}`);
}

main().catch((err) => {
  console.error("❌ FATAL:", err.message);
  process.exit(1);
});
