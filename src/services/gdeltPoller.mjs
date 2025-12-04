// src/services/gdeltPoller.mjs
// FINAL UPGRADED GDELT POLLER (A + B + C + LAT/LON VALIDATION)

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

/* ------------------------------------------------------------------ */
/*  FILTERS + PATTERNS                                                */
/* ------------------------------------------------------------------ */

const BLOCKED_DOMAINS = [
  "tmz.com","people.com","variety.com","hollywoodreporter.com",
  "perezhilton","eonline.com","buzzfeed.com","usmagazine.com",
  "entertainment"
];

// Expanded CAMEO root codes
const ALLOWED_ROOT_CODES = new Set([
  "01","02","03","04","07","08",
  "10","11","12",
  "14","15","16","17","18","19","20"
]);

// Hazard patterns
const HAZARD_PATTERNS = [
  { label: "Severe storm activity", re: /\b(thunderstorm|severe storm|severe weather|damaging wind|line of storms?)\b/i },
  { label: "Winter storm conditions", re: /\b(blizzard|winter storm|snowstorm|snow squall|freezing rain|freezing drizzle)\b/i },
  { label: "Hurricane or tropical system", re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i },
  { label: "Wildfire activity", re: /\b(wild ?fire|bush ?fire|forest fire|brush fire|grass fire)\b/i },
  { label: "Flooding impacts", re: /\b(flood|flooding|flash flood|river flood|urban flooding|coastal flood)\b/i },
  { label: "Earthquake activity", re: /\b(earthquake|aftershock|seismic activity)\b/i },
  { label: "Landslide conditions", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat conditions", re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i },
  { label: "Drought conditions", re: /\b(drought|water shortage|water crisis)\b/i },
  { label: "Power outage", re: /\b(power outage|widespread outage|blackout|loss of power)\b/i },
  { label: "Hazardous material incident", re: /\b(hazmat|chemical spill|toxic leak|gas leak|industrial accident|explosion)\b/i }
];

function detectHazardLabel(text="") {
  for (const {label, re} of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

/* ------------------------------------------------------------------ */
/*  HELPERS                                                           */
/* ------------------------------------------------------------------ */

function parseGdeltDate(sqlDate) {
  return (sqlDate && sqlDate.length===8)
    ? new Date(`${sqlDate.slice(0,4)}-${sqlDate.slice(4,6)}-${sqlDate.slice(6)}T00:00:00Z`)
    : new Date();
}

function getDomain(url) {
  try { return new URL(url).hostname.replace(/^www\./, ""); }
  catch { return ""; }
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  try {
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout:15000, responseType:"text"
    });

    const lines = lastFile.split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find(l => /\d{14}\.export\.CSV\.zip$/.test(l));

    if (!zipLine) {
      console.warn("⚠️ No GDELT file found in lastupdate.txt");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Fetch:", zipUrl);

    const zipResp = await axios.get(zipUrl, {
      responseType:"stream", timeout:60000
    });

    const directory = zipResp.data.pipe(unzipper.Parse({forceStream:true}));
    let csvStream = null;

    for await (const entry of directory) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry; break;
      }
      entry.autodrain();
    }
    if (!csvStream) return console.warn("⚠️ ZIP had no CSV");

    console.log("📄 Parsing CSV…");

    const rl = readline.createInterface({input:csvStream});
    const db = getDB();
    const col = db.collection("social_signals");

    let debugPrinted=0, matched=0, saved=0;

    for await (const line of rl) {
      if (!line.trim()) continue;
      const cols = line.split("\t");
      if (cols.length < 58) continue;

      const sqlDate = cols[1];
      const actor1 = cols[6];
      const actor2 = cols[16];
      const rootCode = cols[28];
      const goldstein = parseFloat(cols[30]);
      const avgTone = parseFloat(cols[34]);

      const fullName = cols[50];
      const country = cols[51];

      let lat = parseFloat(cols[53]);
      let lon = parseFloat(cols[54]);
      const url = cols[57];

      // Fallback Actor1
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        const lat1 = parseFloat(cols[39]);
        const lon1 = parseFloat(cols[40]);
        if (Number.isFinite(lat1) && Number.isFinite(lon1)) {
          lat = lat1; lon = lon1;
        }
      }

      // Fallback Actor2
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        const lat2 = parseFloat(cols[45]);
        const lon2 = parseFloat(cols[46]);
        if (Number.isFinite(lat2) && Number.isFinite(lon2)) {
          lat = lat2; lon = lon2;
        }
      }

      // Debug first 10
      if (debugPrinted < 10) {
        console.log("RAW GDELT:", {sqlDate,fullName,country,root:rootCode,lat,lon,url});
        debugPrinted++;
      }

      // ⛔ NEW: Real latitude/longitude validation
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;

      if (!url) continue;

      const domain = getDomain(url);
      if (BLOCKED_DOMAINS.some(b => domain.includes(b))) continue;

      if (!ALLOWED_ROOT_CODES.has(rootCode)) continue;

      const hazardText = `${fullName} ${actor1} ${actor2} ${url}`.toLowerCase();
      const hazardLabel = detectHazardLabel(hazardText);
      if (!hazardLabel) continue;

      matched++;

      const publishedAt = parseGdeltDate(sqlDate);
      const place = fullName || "Unknown location";

      const doc = {
        type:"news",
        source:"GDELT",
        domain, url,
        title:`${hazardLabel} near ${place}`,
        description:`${hazardLabel} reported near ${place}.`,
        publishedAt,
        createdAt:new Date(),
        expires:new Date(Date.now()+24*60*60*1000),
        geometry:{
          type:"Point",
          coordinates:[lon, lat]
        },
        geometryMethod:"gdelt-geo-fallback",
        gdelt:{ sqlDate, rootCode, goldstein, avgTone, place, actor1, actor2 }
      };

      await col.updateOne({url},{ $set:doc },{upsert:true});
      saved++;
    }

    console.log(`🌍 GDELT DONE — Hazard-matched ${matched}, saved ${saved}`);
  }
  catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
