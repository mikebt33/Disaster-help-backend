// src/services/gdeltPoller.mjs
// GDELT Poller — Upgraded (A+B+C):
// - Falls back to Actor1Geo/Actor2Geo if ActionGeo missing
// - Broadened root codes for real hazard coverage
// - Supports US + international hazard events
// - Better hazard detection across names + URL slugs

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

/* ------------------------------------------------------------------ */
/*  FILTERS + PATTERNS                                                */
/* ------------------------------------------------------------------ */

// Domains we never want (entertainment etc.)
const BLOCKED_DOMAINS = [
  "tmz.com",
  "people.com",
  "variety.com",
  "hollywoodreporter.com",
  "perezhilton",
  "eonline.com",
  "buzzfeed.com",
  "usmagazine.com",
  "entertainment",
];

// Expanded disaster-relevant root codes (CAMEO)
const ALLOWED_ROOT_CODES = new Set([
  "03", // Express intent (often used for disaster warnings)
  "04", // Consult (often disaster communications)
  "07", // Provide aid
  "08", // Yield
  "10", // Demand (common in disaster response)
  "11", // Disapprove
  "12", // Apologize
  "14", // Protest
  "15", // Military posture
  "16", // Reduce relations
  "17", // Coerce
  "18", // Assault
  "19", // Fight
  "20", // Unconventional mass violence
]);

// Global hazard patterns
const HAZARD_PATTERNS = [
  {
    label: "Severe storm activity",
    re: /\b(thunderstorm|severe storm|severe weather|damaging wind|line of storms?)\b/i,
  },
  {
    label: "Winter storm conditions",
    re: /\b(blizzard|winter storm|snowstorm|snow squall|freezing rain|freezing drizzle)\b/i,
  },
  {
    label: "Hurricane or tropical system",
    re: /\b(hurricane|tropical storm|tropical depression|cyclone|typhoon)\b/i,
  },
  {
    label: "Wildfire activity",
    re: /\b(wild ?fire|bush ?fire|forest fire|brush fire|grass fire)\b/i,
  },
  {
    label: "Flooding impacts",
    re: /\b(flood|flooding|flash flood|river flood|urban flooding|coastal flood)\b/i,
  },
  {
    label: "Earthquake activity",
    re: /\b(earthquake|aftershock|seismic activity)\b/i,
  },
  {
    label: "Landslide conditions",
    re: /\b(landslide|mudslide|debris flow)\b/i,
  },
  {
    label: "Extreme heat conditions",
    re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i,
  },
  {
    label: "Drought conditions",
    re: /\b(drought|water shortage|water crisis)\b/i,
  },
  {
    label: "Power outage",
    re: /\b(power outage|widespread outage|blackout|loss of power)\b/i,
  },
  {
    label: "Hazardous material incident",
    re: /\b(hazmat|chemical spill|toxic leak|gas leak|industrial accident|explosion)\b/i,
  },
];

function detectHazardLabel(text = "") {
  if (!text) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(text)) return label;
  }
  return null;
}

/* ------------------------------------------------------------------ */
/*  HELPERS                                                           */
/* ------------------------------------------------------------------ */

function parseGdeltDate(sqlDate) {
  return sqlDate && sqlDate.length === 8
    ? new Date(`${sqlDate.slice(0, 4)}-${sqlDate.slice(4, 6)}-${sqlDate.slice(6)}T00:00:00Z`)
    : new Date();
}

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

/* Allow INTERNATIONAL events */
function isRelevantLocation(fullName = "", countryCode = "") {
  if (!fullName) return false;

  const name = fullName.toLowerCase();

  // Always accept US
  if (countryCode === "US") return true;
  if (name.includes("united states") || name.includes("usa")) return true;

  // Global major regions
  if (/europe|asia|africa|pacific|latin america|caribbean|middle east/i.test(name)) return true;

  // Accept any country with hazard contexts (we infer by coordinates later anyway)
  return true;
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running (Upgraded)…");

  try {
    /* ----------------- 1. Determine latest file ----------------- */
    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
    });

    const lines = lastFile.split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => /\d{14}\.export\.CSV\.zip$/.test(l));

    if (!zipLine) {
      console.warn("⚠️ No valid GDELT export ZIP in lastupdate.txt.");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading GDELT ZIP:", zipUrl);

    /* ----------------- 2. Download ZIP ----------------- */
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
    });

    /* ----------------- 3. Extract CSV ----------------- */
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
      console.warn("⚠️ GDELT ZIP had no CSV.");
      return;
    }

    console.log("📄 Parsing CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let debugPrinted = 0;
    let matched = 0;
    let saved = 0;

    const MIN_COLUMNS = 58;

    /* ----------------- 4. Main loop ----------------- */
    for await (const line of rl) {
      if (!line.trim()) continue;
      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      // Extract main GDELT fields
      const sqlDate = cols[1];
      const actor1Name = cols[6];
      const actor2Name = cols[16];
      const eventRootCode = cols[28];
      const goldstein = parseFloat(cols[30]);
      const avgTone = parseFloat(cols[34]);

      const fullName = cols[50];
      const country = cols[51];

      let lat = parseFloat(cols[53]);
      let lon = parseFloat(cols[54]);

      const url = cols[57];

      /* --- (A) Fallback to Actor1Geo if ActionGeo missing --- */
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        const a1Lat = parseFloat(cols[39]); // Actor1Geo_Lat
        const a1Lon = parseFloat(cols[40]); // Actor1Geo_Long
        if (Number.isFinite(a1Lat) && Number.isFinite(a1Lon)) {
          lat = a1Lat;
          lon = a1Lon;
        }
      }

      /* --- (A) Fallback to Actor2Geo if still missing --- */
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        const a2Lat = parseFloat(cols[45]); // Actor2Geo_Lat
        const a2Lon = parseFloat(cols[46]); // Actor2Geo_Long
        if (Number.isFinite(a2Lat) && Number.isFinite(a2Lon)) {
          lat = a2Lat;
          lon = a2Lon;
        }
      }

      /* Debug first 10 only */
      if (debugPrinted < 10) {
        console.log("RAW GDELT:", {
          sqlDate,
          fullName,
          country,
          root: eventRootCode,
          goldstein,
          avgTone,
          url,
          lat,
          lon,
        });
        debugPrinted++;
      }

      /* -- Allow INTERNATIONAL hazard events (Option C) -- */
      if (!isRelevantLocation(fullName, country)) continue;

      /* Must have coordinates now */
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

      if (!url) continue;

      const domain = getDomain(url);
      if (BLOCKED_DOMAINS.some((b) => domain.includes(b))) continue;

      /* Expanded root codes (Option B) */
      if (!ALLOWED_ROOT_CODES.has(eventRootCode)) continue;

      /* Hazard detection improved (Option C) */
      const hazardText = [
        fullName || "",
        actor1Name || "",
        actor2Name || "",
        url || "",
      ]
        .join(" ")
        .toLowerCase();

      const hazardLabel = detectHazardLabel(hazardText);
      if (!hazardLabel) continue;

      matched++;

      /* Build document */
      const publishedAt = parseGdeltDate(sqlDate);
      const place = fullName || "Unknown location";

      const doc = {
        type: "news",
        source: "GDELT",
        domain,
        url,

        title: `${hazardLabel} near ${place}`,
        description: `${hazardLabel} reported near ${place}.`,

        publishedAt,
        createdAt: new Date(),
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000),

        geometry: {
          type: "Point",
          coordinates: [lon, lat],
        },
        geometryMethod: "gdelt-geo-fallback",

        gdelt: {
          sqlDate,
          eventRootCode,
          goldstein,
          avgTone,
          place,
          actor1Name,
          actor2Name,
        },
      };

      await col.updateOne({ url }, { $set: doc }, { upsert: true });
      saved++;
    }

    console.log(
      `🌎 GDELT DONE — Hazard-matched ${matched} events, saved ${saved} documents.`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
