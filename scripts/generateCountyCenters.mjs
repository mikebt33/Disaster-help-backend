import fs from "fs";
import path from "path";
import fetch from "node-fetch";
import shp from "shpjs"; // install this with npm install shpjs node-fetch

// U.S. Census TIGER/Line counties shapefile
const TIGER_URL =
  "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip";

const outputPath = path.resolve("./src/data/county_centers.json");

(async () => {
  console.log("📦 Downloading TIGER county shapefile...");
  const res = await fetch(TIGER_URL);
  const buffer = await res.arrayBuffer();

  console.log("📂 Parsing shapefile...");
  const geojson = await shp(buffer);

  const centers = {};

  console.log("🧮 Computing county centroids...");
  const stateMap = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR"
  };

  for (const f of geojson.features) {
    const props = f.properties;
    const state = stateMap[props.STATEFP];
    if (!state) continue;

    const county = props.NAME.trim();
    const coords = f.geometry.coordinates.flat(Infinity);
    const lons = coords.filter((_, i) => i % 2 === 0);
    const lats = coords.filter((_, i) => i % 2 === 1);
    const lonAvg = lons.reduce((a, b) => a + b, 0) / lons.length;
    const latAvg = lats.reduce((a, b) => a + b, 0) / lats.length;

    if (!centers[state]) centers[state] = {};
    centers[state][county] = [parseFloat(lonAvg.toFixed(4)), parseFloat(latAvg.toFixed(4))];
  }

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(centers, null, 2));
  console.log(`✅ Saved county centers for ${Object.keys(centers).length} states → ${outputPath}`);
})();
