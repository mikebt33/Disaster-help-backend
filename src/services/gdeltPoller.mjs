// src/services/gdeltPoller.mjs
// GDELT Poller — Strict Disaster/Weather-Only Mode
// Produces clean, civilian-friendly quicksheet text, stored in `social_signals`.

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

/* ------------------------------------------------------------------ */
/*  DOMAIN + HAZARD FILTERING                                         */
/* ------------------------------------------------------------------ */

// Domains we never want (celebrity / gossip / pure entertainment).
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

// Root codes we’re willing to consider. These are the “non-trivial”,
// higher-impact interactions in CAMEO / GDELT.
const ALLOWED_ROOT_CODES = new Set([
  "07", // Provide aid
  "08", // Yield
  "14", // Protest
  "15", // Military posture
  "16", // Reduce relations
  "17", // Coerce
  "18", // Assault
  "19", // Fight
  "20", // Unconventional mass violence
]);

// Strict hazard patterns: all have word boundaries so we don’t match
// “police” -> “ice”, “heath” -> “heat”, etc.
const HAZARD_PATTERNS = [
  {
    label: "Severe storm activity",
    re: /\b(thunderstorm(s)?|severe storm(s)?|severe weather|strong storm(s)?|damaging wind(s)?|strong wind(s)?|line of storms?)\b/i,
  },
  {
    label: "Winter storm conditions",
    re: /\b(blizzard(s)?|winter storm(s)?|snowstorm(s)?|snow squall(s)?|lake-effect snow|winter weather advisory|snow (accumulation|squall(s)?)|freezing rain|freezing drizzle)\b/i,
  },
  {
    label: "Hurricane or tropical system",
    re: /\b(hurricane(s)?|tropical storm(s)?|tropical depression(s)?|cyclone(s)?|typhoon(s)?)\b/i,
  },
  {
    label: "Wildfire activity",
    re: /\b(wild ?fire(s)?|bush ?fire(s)?|forest fire(s)?|brush fire(s)?|grass fire(s)?)\b/i,
  },
  {
    label: "Flooding impacts",
    re: /\b(flood(s)?|flooding|flash flood(s)?|river flood(s)?|urban flood(s)?|coastal flood(s)?)\b/i,
  },
  {
    label: "Earthquake activity",
    re: /\b(earthquake(s)?|aftershock(s)?|seismic activity)\b/i,
  },
  {
    label: "Landslide conditions",
    re: /\b(landslide(s)?|mudslide(s)?|debris flow(s)?)\b/i,
  },
  {
    label: "Extreme heat conditions",
    re: /\b(heat wave(s)?|extreme heat|dangerous heat|record heat|oppressive heat)\b/i,
  },
  {
    label: "Drought conditions",
    re: /\b(drought(s)?|water shortage(s)?|water crisis)\b/i,
  },
  {
    label: "Power outage",
    re: /\b(power outage(s)?|widespread outage(s)?|blackout(s)?|loss of power|power cut(s)?)\b/i,
  },
  {
    label: "Hazardous material incident",
    re: /\b(hazmat|chemical spill(s)?|toxic (leak|spill)|gas leak(s)?|industrial accident(s)?|plant explosion(s)?|chemical explosion(s)?|explosion(s)?)\b/i,
  },
];

// Given some text (location, actors, URL slug), return a hazard label
// or null if nothing looks like a disaster/weather hazard.
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
  if (!sqlDate || sqlDate.length !== 8) return new Date();
  // SQLDATE is YYYYMMDD
  return new Date(
    `${sqlDate.slice(0, 4)}-${sqlDate.slice(4, 6)}-${sqlDate.slice(6)}T00:00:00Z`
  );
}

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

// Very US-focused filter – we only want US incidents right now.
function isUSLocation(fullName = "", countryCode = "") {
  const name = fullName.toLowerCase();
  if (countryCode === "US") return true;

  if (name.includes("united states") || name.includes("usa")) return true;

  // Quick state abbrev check in the location string.
  if (
    name.match(
      /\b(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV)\b/i
    )
  ) {
    return true;
  }

  return false;
}

/* ------------------------------------------------------------------ */
/*  MAIN POLLER                                                       */
/* ------------------------------------------------------------------ */

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running (Strict Disaster-Only)…");

  try {
    /* ----------------- 1. Figure out latest events file ----------------- */

    const { data: lastFile } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
    });

    const lines = lastFile.split(/\r?\n/).filter(Boolean);
    const zipLine = lines.find((l) => l.match(/\/\d{14}\.export\.CSV\.zip$/));

    if (!zipLine) {
      console.warn("⚠️ No valid GDELT events export ZIP found in lastupdate.txt.");
      return;
    }

    const zipUrl = zipLine.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading GDELT events ZIP:", zipUrl);

    /* ----------------- 2. Download ZIP ----------------- */

    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
    });

    /* ----------------- 3. Extract CSV from ZIP ----------------- */

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
      console.warn("⚠️ GDELT ZIP contained no CSV file.");
      return;
    }

    console.log("📄 Parsing GDELT CSV…");

    const rl = readline.createInterface({ input: csvStream });
    const db = getDB();
    const col = db.collection("social_signals");

    let debugPrinted = 0;
    let matched = 0;
    let saved = 0;

    // Column indices for GDELT Events v2.0
    //  0: GlobalEventID
    //  1: SQLDATE
    //  ...
    // 28: EventRootCode
    // 30: GoldsteinScale
    // 34: AvgTone
    // 36–41: Actor1Geo_*
    // 43–48: Actor2Geo_*
    // 50–55: ActionGeo_*
    // 57: SOURCEURL
    const MIN_COLUMNS = 58; // safety

    /* ----------------- 4. Main CSV loop ----------------- */

    for await (const line of rl) {
      if (!line.trim()) continue;
      const cols = line.split("\t");

      if (cols.length < MIN_COLUMNS) {
        // Probably the wrong file type; skip.
        continue;
      }

      // Pull out relevant fields
      const sqlDate = cols[1]; // SQLDATE
      const actor1Name = cols[6]; // Actor1Name
      const actor2Name = cols[16]; // Actor2Name
      const eventRootCode = cols[28]; // EventRootCode
      const goldstein = parseFloat(cols[30]); // GoldsteinScale
      const avgTone = parseFloat(cols[34]); // AvgTone

      const fullName = cols[50]; // ActionGeo_FullName
      const country = cols[51]; // ActionGeo_CountryCode
      const lat = parseFloat(cols[53]); // ActionGeo_Lat
      const lon = parseFloat(cols[54]); // ActionGeo_Long
      const url = cols[57]; // SOURCEURL

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

      // ---------- FILTER 1: Must be in/clearly about the US ----------
      if (!isUSLocation(fullName, country)) continue;

      // ---------- FILTER 2: Must have coordinates ----------
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

      // ---------- FILTER 3: Must have a URL ----------
      if (!url) continue;

      const domain = getDomain(url);

      // ---------- FILTER 4: Block gossip/entertainment domains ----------
      if (BLOCKED_DOMAINS.some((b) => domain.includes(b))) continue;

      // ---------- FILTER 5: Limit to higher-impact root codes ----------
      if (!ALLOWED_ROOT_CODES.has(eventRootCode)) continue;

      // ---------- FILTER 6: Strict hazard keyword match ----------
      // Use *location + actor names + URL path* for hazard detection.
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

      // ---------- Build document ----------

      const publishedAt = parseGdeltDate(sqlDate);
      const place = fullName || "Unknown location";

      const title = `${hazardLabel} near ${place}`;
      const description = `${hazardLabel} reported near ${place}.`;

      const doc = {
        type: "news",
        source: "GDELT",
        domain,
        url,

        title,
        description,

        publishedAt,
        createdAt: new Date(),
        // GDELT events are very short-lived; 24h retention is fine for UI.
        expires: new Date(Date.now() + 24 * 60 * 60 * 1000),

        geometry: {
          type: "Point",
          coordinates: [lon, lat],
        },
        geometryMethod: "gdelt-action-geo",

        gdelt: {
          sqlDate,
          eventRootCode,
          goldstein,
          avgTone,
          fullName,
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
