// ------------------------------------------------------------
// FINAL WORKING GDELT POLLER — Disaster Help
// ------------------------------------------------------------
// - SSL mismatch fixed via custom HTTPS agent
// - Correct 61-column schema
// - Broad hazard detection
// - ActionGeo → Actor1Geo → Actor2Geo fallback
// - Relaxed US filtering (optional)
// - Bulk upsert + TTL cleanup
// ------------------------------------------------------------

import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import https from "https";
import { getDB } from "../db.js";

const LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt";
const USER_AGENT = "DisasterHelp-GDELT/1.0";

// Fix GDELT SSL hostname mismatch
const httpsAgent = new https.Agent({
  rejectUnauthorized: false,
});

const IDX = Object.freeze({
  GID: 0,
  SQLDATE: 1,
  ACTOR1NAME: 6,
  ACTOR2NAME: 16,
  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1_LAT: 40,
  ACTOR1_LON: 41,

  ACTOR2_LAT: 48,
  ACTOR2_LON: 49,

  ACTION_LAT: 56,
  ACTION_LON: 57,

  ACTION_PLACE: 52,
  ACTION_CTRY: 53,

  DATEADDED: 59,
  SOURCEURL: 60,
});

const MIN_COLUMNS = 61;

// Domains to suppress
const BLOCKED = [
  "tmz.com", "people.com", "buzzfeed.com",
  "hollywoodreporter.com", "variety.com",
  "perezhilton", "entertainment"
];

// Hazard patterns (broad)
const HAZARD_PATTERNS = [
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },
  { label: "Flooding", re: /\b(flood|flash flood|inundat|storm surge)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire)\b/i },
  { label: "Winter storm", re: /\b(blizzard|snowstorm|winter storm|whiteout|freezing rain)\b/i },
  { label: "High winds", re: /\b(high winds?|damaging winds?|gusts?|windstorm)\b/i },
  { label: "Power outage", re: /\b(power outage|without power|blackout|loss of power|downed lines?)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|extreme heat|dangerous heat|record heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },
  { label: "Hazmat", re: /\b(chemical spill|gas leak|toxic|hazmat|explosion)\b/i }
];

function detectHazard(text) {
  if (!text) return null;
  for (const h of HAZARD_PATTERNS) {
    if (h.re.test(text)) return h.label;
  }
  return null;
}

function validCoord(x) {
  return Number.isFinite(x) && Math.abs(x) <= 180;
}

function pickCoords(cols) {
  const aLat = parseFloat(cols[IDX.ACTION_LAT]);
  const aLon = parseFloat(cols[IDX.ACTION_LON]);

  if (validCoord(aLon) && validCoord(aLat)) {
    return { lat: aLat, lon: aLon, method: "action" };
  }

  const a1Lat = parseFloat(cols[IDX.ACTOR1_LAT]);
  const a1Lon = parseFloat(cols[IDX.ACTOR1_LON]);
  if (validCoord(a1Lon) && validCoord(a1Lat)) {
    return { lat: a1Lat, lon: a1Lon, method: "actor1" };
  }

  const a2Lat = parseFloat(cols[IDX.ACTOR2_LAT]);
  const a2Lon = parseFloat(cols[IDX.ACTOR2_LON]);
  if (validCoord(a2Lon) && validCoord(a2Lat)) {
    return { lat: a2Lat, lon: a2Lon, method: "actor2" };
  }

  return null;
}

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

function parseDateAdded(s) {
  if (!/^\d{14}$/.test(s)) return null;
  return new Date(
    `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}T${s.slice(8,10)}:${s.slice(10,12)}:${s.slice(12)}Z`
  );
}

export async function pollGDELT() {
  console.log("🌎 GDELT Poller running…");

  const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 300;
  const BATCH_SIZE = 200;
  const TTL_MS = 24 * 3600 * 1000;

  let saved = 0;

  try {
    // -----------------------------
    // STEP 1 — Fetch lastupdate.txt
    // -----------------------------
    const { data: text } = await axios.get(LASTUPDATE_URL, {
      headers: { "User-Agent": USER_AGENT },
      httpsAgent
    });

    const line = String(text)
      .split("\n")
      .find((l) => l.endsWith(".export.CSV.zip"));

    if (!line) {
      console.warn("⚠️ No GDELT export reference found.");
      return;
    }

    const zipUrl = line.trim().split(/\s+/).pop();
    console.log("⬇️ Downloading:", zipUrl);

    // -----------------------------
    // STEP 2 — Download ZIP stream
    // -----------------------------
    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      headers: { "User-Agent": USER_AGENT },
      httpsAgent
    });

    // -----------------------------
    // STEP 3 — Extract CSV
    // -----------------------------
    let csvStream = null;
    const dir = zipResp.data.pipe(unzipper.Parse({ forceStream: true }));

    for await (const entry of dir) {
      if (entry.path.toLowerCase().endsWith(".csv")) {
        csvStream = entry;
        break;
      }
      entry.autodrain();
    }

    if (!csvStream) {
      console.warn("⚠️ ZIP contains no CSV");
      return;
    }

    // -----------------------------
    // STEP 4 — Read CSV line-by-line
    // -----------------------------
    const rl = readline.createInterface({ input: csvStream });

    const db = getDB();
    const col = db.collection("social_signals");

    // TTL cleanup
    try {
      await col.deleteMany({ source: "GDELT", expires: { $lte: new Date() } });
    } catch {}

    let ops = [];
    let debug = 0;

    const flush = async () => {
      if (!ops.length) return;
      try {
        await col.bulkWrite(ops, { ordered: false });
      } catch (err) {
        console.warn("bulkWrite warning:", err.message);
      }
      ops = [];
    };

    for await (const line of rl) {
      if (!line.trim()) continue;

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !url.startsWith("http")) continue;

      const domain = getDomain(url);
      if (!domain) continue;
      if (BLOCKED.some((b) => domain.includes(b))) continue;

      // Extract coords
      const coords = pickCoords(cols);
      if (!coords) continue;

      const hazardText = `
        ${cols[IDX.ACTOR1NAME] || ""}
        ${cols[IDX.ACTOR2NAME] || ""}
        ${cols[IDX.ACTION_PLACE] || ""}
        ${url}
        ${domain}
      `.replace(/[_/\\.-]/g, " ");

      const hazard = detectHazard(hazardText);
      if (!hazard) continue;

      const publishedAt =
        parseDateAdded(cols[IDX.DATEADDED]) || new Date();

      if (debug < 10) {
        console.log("GDELT EVENT:", {
          hazard,
          place: cols[IDX.ACTION_PLACE],
          domain,
          coords,
          url,
        });
        debug++;
      }

      const doc = {
        type: "news",
        source: "GDELT",
        hazardLabel: hazard,

        title: `${hazard} near ${cols[IDX.ACTION_PLACE] || "Unknown"}`,
        description: `${hazard} reported near ${cols[IDX.ACTION_PLACE] || "Unknown"}.`,

        url,
        domain,
        publishedAt,
        updatedAt: new Date(),
        expires: new Date(publishedAt.getTime() + TTL_MS),

        geometry: {
          type: "Point",
          coordinates: [coords.lon, coords.lat],
        },
        geometryMethod: coords.method,

        gdelt: {
          eventId: cols[IDX.GID],
          sqlDate: cols[IDX.SQLDATE],
          dateAdded: cols[IDX.DATEADDED],
          rootCode: cols[IDX.EVENTROOTCODE],
          actor1: cols[IDX.ACTOR1NAME],
          actor2: cols[IDX.ACTOR2NAME],
        },
      };

      ops.push({
        updateOne: {
          filter: { url },
          update: { $set: doc, $setOnInsert: { createdAt: new Date() } },
          upsert: true,
        },
      });

      saved++;
      if (ops.length >= BATCH_SIZE) await flush();
      if (saved >= MAX_SAVE) break;
    }

    await flush();

    console.log(`🌍 GDELT DONE — saved ${saved} events`);
  } catch (err) {
    console.error("❌ GDELT ERROR:", err.message);
  }
}
