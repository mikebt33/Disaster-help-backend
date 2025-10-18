// news/socialNewsPoller.mjs (or .js)
// Strict U.S.-only disaster poller with high-precision state mapping.
// Adds Atlantic vs Gulf coastal split, AK/HI handling, safe regex rebuilds, and tiny jitter.

import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

console.log("NEWS_API_KEY loaded:", !!process.env.NEWS_API_KEY);
const DEBUG = !!process.env.DEBUG_NEWS_GEO;

// -------------------- Load county centers --------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

// Precompute conservative state centroids from counties
const STATE_CENTROIDS = {};
for (const [st, counties] of Object.entries(countyCenters)) {
  const coords = Object.values(counties).filter((v) => Array.isArray(v));
  if (coords.length) {
    const avgLon = coords.reduce((s, c) => s + c[0], 0) / coords.length;
    const avgLat = coords.reduce((s, c) => s + c[1], 0) / coords.length;
    STATE_CENTROIDS[st] = [avgLon, avgLat];
  }
}

// -------------------- Config --------------------
const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

// State name ↔ code maps (50 states only; AK & HI included)
const STATE_NAME_TO_CODE = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR", california: "CA",
  colorado: "CO", connecticut: "CT", delaware: "DE", florida: "FL", georgia: "GA",
  hawaii: "HI", idaho: "ID", illinois: "IL", indiana: "IN", iowa: "IA",
  kansas: "KS", kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD",
  massachusetts: "MA", michigan: "MI", minnesota: "MN", mississippi: "MS",
  missouri: "MO", montana: "MT", nebraska: "NE", nevada: "NV", "new hampshire": "NH",
  "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
  "north dakota": "ND", ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA",
  "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
  tennessee: "TN", texas: "TX", utah: "UT", vermont: "VT", virginia: "VA",
  washington: "WA", "west virginia": "WV", wisconsin: "WI", wyoming: "WY"
};
const STATE_CODES = new Set(Object.values(STATE_NAME_TO_CODE));
const STATE_NAMES = new Set(Object.keys(STATE_NAME_TO_CODE));

// -------------------- Hazard & filters --------------------
// Strong, literal disaster signals only (avoid figurative).
const HAZARD_WORDS = [
  "flash flood", "storm surge", "hurricane", "tropical storm", "tornado",
  "cyclone", "typhoon", "wildfire", "forest fire", "earthquake", "aftershock",
  "tsunami", "volcano", "eruption", "landslide", "mudslide", "blizzard",
  "heat wave", "heatwave", "drought", "avalanche", "hailstorm", "dust storm",
  "severe weather", "snowstorm", "ice storm", "power outage", "blackout",
  "floods", "flooding"
];

// Build a safe source string and factory to avoid global RegExp state bugs
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const HAZARD_RE_SOURCE =
  "\\b(" + HAZARD_WORDS.sort((a, b) => b.length - a.length).map(esc).join("|") + ")\\b";
const makeHazardRe = (flags) => new RegExp(HAZARD_RE_SOURCE, flags);

// Noise / non-disaster topics (explicit)
const HARD_BLOCK = /(\b(taylor swift|grammy|oscars?|hollywood|netflix|museum|painting|concert|celebrity|theater|movie|album|music video)\b)|(\b(senate|congress|election|campaign|supreme court|lawsuit|indictment|democrat|republican|white house|trump|biden)\b)|(\b(stock|market|loan|bailout|stimulus|bond|treasury|fiscal|budget)\b)|(\b(shooting|murder|assault|homicide|kidnapping|smuggler|cartel)\b)/i;

// Figurative/idiomatic uses of hazard terms
const FIGURATIVE = /\bfans?\s+flood|sales?\s+flood|tweets?\s+flood|inbox\s+flood|orders?\s+flood|applications?\s+flood/i;

// Non-U.S. locations/contexts to exclude (conservative)
const FOREIGN = /\b(puerto rico|mexico|canada|philippines?|argentina|brazil|colombia|chile|peru|haiti|jamaica|dominican|cuba|venezuela|europe|asia|africa|australia|new zealand|uk|britain|england|ireland|scotland|france|germany|spain|italy|india|china|japan|pakistan|afghanistan|russia|ukraine|israel|gaza|iran|iraq|syria|lebanon|yemen|egypt|nigeria|somalia)\b/i;

// Words that validate a *real-world* hazard context near the hazard word
const CONTEXT_VALIDATORS = /\b(storm|rain|rains|rainfall|wind|winds|mph|gusts|surge|inches|river|creek|coast|coastal|shore|evacuations?|shelters?|rescued?|guard|national guard|coast guard|emergency|nws|weather service|forecast|warning|watch|advisory|landfall|damage|inundation|mud|debris|burn|firefighters?)\b/i;

// Treat “rescue/evacuation” alone as insufficient (must co-occur with a real hazard)
const WEAK_ALONE = /\b(rescue|rescues|rescued|evacuation|evacuations|evacuate|evacuated)\b/i;

// Washington special cases (avoid DC/post)
const WASHINGTON_FALSE = /\b(washington post|washington,\s*d\.?c\.?|washington dc)\b/i;
// Accept “Washington state”, “western/eastern Washington”, etc.
const WASHINGTON_STRICT = /\b(washington state|western washington|eastern washington|state of washington)\b/i;

// -------------------- US bounds --------------------
function inUSBounds(lon, lat) {
  // CONUS
  const conus = lat >= 24 && lat <= 49.5 && lon >= -125 && lon <= -66.5;
  // Alaska (very rough but conservative)
  const alaska = lat >= 51 && lat <= 71.8 && lon >= -179.2 && lon <= -129;
  // Hawaii
  const hawaii = lat >= 18.8 && lat <= 22.4 && lon >= -160.6 && lon <= -154.4;
  return conus || alaska || hawaii;
}

// Optional tiny jitter to avoid exact pin stacking (kept conservative)
function jitter(coords, deg = 0.15) {
  const [lon, lat] = coords;
  return [lon + (Math.random() - 0.5) * deg, lat + (Math.random() - 0.5) * deg];
}

// -------------------- State token helpers --------------------
function toStateCode(token) {
  if (!token) return null;
  const t = token.trim();
  if (STATE_CODES.has(t.toUpperCase())) return t.toUpperCase();
  const n = t.toLowerCase();
  if (STATE_NAME_TO_CODE[n]) return STATE_NAME_TO_CODE[n];
  return null;
}

// Build state regexes per call (avoid global lastIndex issues)
const makeStateAbbrRe = () => new RegExp("\\b(" + Array.from(STATE_CODES).join("|") + ")\\b", "g");
const makeStateFullRe = () => new RegExp("\\b(" + Array.from(STATE_NAMES).join("|") + ")\\b", "gi");

// -------------------- Coastal heuristic helpers --------------------
const ATLANTIC_HINT = /(atlantic|outer banks|mid-atlantic|east coast|bermuda|bahamas)/i;
const GULF_HINT = /\bgulf\b|\bgulf of mexico\b|\bpanhandle\b|\byucat[aá]n\b/i;

const ATLANTIC_STATES = ["FL","GA","SC","NC","VA","MD","DE","NJ","NY","CT","RI","MA","ME"];
const GULF_STATES = ["TX","LA","MS","AL","FL"];

function pickFromMentioned(text, candidateStates) {
  const abbrRe = makeStateAbbrRe();
  const fullRe = makeStateFullRe();
  const tokens = [
    ...text.matchAll(abbrRe),
    ...text.matchAll(fullRe),
  ].map((m) => toStateCode(m[0]) || "");

  const score = {};
  for (const t of tokens) {
    if (candidateStates.includes(t)) score[t] = (score[t] || 0) + 1;
  }
  const best = Object.entries(score).sort((a, b) => b[1] - a[1])[0];
  return best ? best[0] : null;
}

// -------------------- Location extraction (conservative) --------------------
/**
 * Extract a US state (and optionally a county within that state) from text,
 * but only when the location appears *near* a real hazard mention.
 */
function extractLocation(textRaw) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  // 1) Must contain a real hazard term
  const hazardMatches = [...text.matchAll(makeHazardRe("gi"))];
  if (hazardMatches.length === 0) return null;

  // Reject hard noise, foreign contexts, figurative uses
  if (HARD_BLOCK.test(text) || FOREIGN.test(text) || FIGURATIVE.test(text)) {
    if (DEBUG) console.log("🧹 Drop by block/foreign/figurative");
    return null;
  }

  // Require validating weather/emergency context near at least one hazard
  let hasValidatedHazard = false;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 100);
    const end = Math.min(text.length, m.index + m[0].length + 100);
    const window = text.slice(start, end);
    if (CONTEXT_VALIDATORS.test(window) || !WEAK_ALONE.test(window)) {
      hasValidatedHazard = true;
      break;
    }
  }
  if (!hasValidatedHazard) {
    if (DEBUG) console.log("🧹 Drop: hazard lacks validating context");
    return null;
  }

  // 2) Conservative AK/HI explicit mapping
  if (/\balaska\b/.test(text) && STATE_CENTROIDS.AK) {
    if (DEBUG) console.log("🧭 Alaska explicit → AK");
    return { type: "Point", coordinates: jitter(STATE_CENTROIDS.AK, 0.1), method: "state", state: "AK", confidence: 3 };
  }
  if (/\bhawaii\b/.test(text) && STATE_CENTROIDS.HI) {
    if (DEBUG) console.log("🧭 Hawaii explicit → HI");
    return { type: "Point", coordinates: jitter(STATE_CENTROIDS.HI, 0.1), method: "state", state: "HI", confidence: 3 };
  }

  // 3) County, ST or County, StateName (only when explicitly paired)
  const countyRe = /([A-Za-z.\- ']+?)\s+county(?:\s+[\w'.-]+)?\s*,\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|[A-Za-z ]{4,})\b/gi;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 140);
    const end = Math.min(text.length, m.index + m[0].length + 140);
    const window = text.slice(start, end);

    let cm;
    while ((cm = countyRe.exec(window))) {
      const countyName = cm[1].trim().replace(/\s+Sheriff.*$/i, "");
      const stCode = toStateCode(cm[2]);
      if (stCode && countyCenters[stCode] && countyCenters[stCode][countyName]) {
        const coords = countyCenters[stCode][countyName];
        if (Array.isArray(coords) && inUSBounds(coords[0], coords[1])) {
          if (DEBUG) console.log(`📍 County match → ${countyName}, ${stCode}`);
          return { type: "Point", coordinates: jitter(coords, 0.08), method: "county", state: stCode, confidence: 3 };
        }
      }
    }
  }

  // 4) City/Place, ST|State — style datelines (keep the state only)
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 140);
    const end = Math.min(text.length, m.index + m[0].length + 140);
    const window = text.slice(start, end);
    const datelineRe = /[,–—-]\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|[A-Za-z ]{4,})\b/gi;

    let d;
    while ((d = datelineRe.exec(window))) {
      let token = d[1].trim();
      // Washington special handling
      if (/^washington$/i.test(token) && !WASHINGTON_STRICT.test(window)) continue;
      if (WASHINGTON_FALSE.test(window)) continue;

      const stCode = toStateCode(token);
      if (stCode && STATE_CENTROIDS[stCode]) {
        if (DEBUG) console.log(`🧭 Dateline → ${stCode}`);
        return { type: "Point", coordinates: jitter(STATE_CENTROIDS[stCode], 0.15), method: "state", state: stCode, confidence: 2 };
      }
    }
  }

  // 5) Basin-aware coastal heuristic (very conservative)
  const mentionsHurricane = /(hurricane|tropical storm|storm surge)/i.test(text);

  if (mentionsHurricane && GULF_HINT.test(text)) {
    // Gulf preference: choose mentioned coastal state or default to TX
    const picked = pickFromMentioned(text, GULF_STATES) || "TX";
    if (STATE_CENTROIDS[picked]) {
      if (DEBUG) console.log(`🌊 Gulf context → ${picked}`);
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[picked], 0.2), method: "coastal-heuristic", state: picked, confidence: 2 };
    }
  }

  if (mentionsHurricane && ATLANTIC_HINT.test(text)) {
    // Atlantic preference: choose mentioned coastal state or default to FL
    const picked = pickFromMentioned(text, ATLANTIC_STATES) || "FL";
    if (STATE_CENTROIDS[picked]) {
      if (DEBUG) console.log(`🌀 Atlantic context → ${picked}`);
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[picked], 0.2), method: "coastal-heuristic", state: picked, confidence: 2 };
    }
  }

  // 6) Plain state inference near hazards
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 140);
    const end = Math.min(text.length, m.index + m[0].length + 140);
    const window = text.slice(start, end);
    const abbrRe = makeStateAbbrRe();
    const fullRe = makeStateFullRe();

    const simpleStateTokens = [
      ...window.matchAll(abbrRe),
      ...window.matchAll(fullRe),
    ].map((mm) => mm[0]);

    const counts = {};
    for (const tok of simpleStateTokens) {
      if (/^washington$/i.test(tok) && !WASHINGTON_STRICT.test(window)) continue;
      const stCode = toStateCode(tok);
      if (!stCode) continue;
      counts[stCode] = (counts[stCode] || 0) + 1;
    }
    const candidate = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    if (candidate && STATE_CENTROIDS[candidate[0]]) {
      if (DEBUG) console.log(`📍 Simple state → ${candidate[0]}`);
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[candidate[0]], 0.15), method: "state", state: candidate[0], confidence: 2 };
    }
  }

  if (DEBUG) console.log("🧹 Drop: no reliable US location");
  return null;
}

// -------------------- Normalize & filter an article --------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const content = (article.content || "").trim();
    const text = `${title}\n${desc}\n${content}`;

    if (HARD_BLOCK.test(text) || FOREIGN.test(text) || FIGURATIVE.test(text)) {
      if (DEBUG) console.log("🧹 Drop early (block/foreign/figurative):", title);
      return null;
    }

    // Use a *fresh* regex (no /g state bug)
    if (!makeHazardRe("i").test(text)) {
      if (DEBUG) console.log("🧹 Drop: no strict hazard:", title);
      return null;
    }

    const geometry = extractLocation(text);
    if (!geometry) {
      if (DEBUG) console.log("🧹 Drop: no geometry:", title);
      return null;
    }

    let domain = "";
    try {
      const u = new URL(article.url);
      domain = u.hostname.replace(/^www\./, "");
    } catch (_) {}

    return {
      title: title || "News Update",
      description: desc,
      url: article.url || "",
      source: article.source?.name || domain || "Unknown",
      domain,
      publishedAt: new Date(article.publishedAt || Date.now()),
      geometry,
      geometryMethod: geometry.method,
      timestamp: new Date(),
      expires: new Date(Date.now() + 24 * 60 * 60 * 1000) // 24h TTL
    };
  } catch (err) {
    console.warn("❌ Error normalizing article:", err.message);
    return null;
  }
}

// -------------------- Save to Mongo --------------------
async function saveNewsArticles(articles) {
  try {
    const db = getDB();
    const col = db.collection("social_signals");
    // Clean expired
    await col.deleteMany({ expires: { $lte: new Date() } });

    for (const a of articles) {
      // Upsert by URL; ignore if missing URL
      if (!a.url) continue;
      await col.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    }
    console.log(`💾 Saved ${articles.length} news articles`);
  } catch (err) {
    console.error("❌ Error saving news articles:", err.message);
  }
}

// -------------------- Poller main --------------------
export async function pollNewsAPI() {
  console.log("📰 News Poller running (strict U.S. disaster focus; conservative mapping)…");
  try {
    // Compose a stable, high-signal query (avoid weak tokens like plain "flood")
    const queryTokens = [
      "flash flood", "storm surge", "hurricane", "tropical storm",
      "tornado", "wildfire", "earthquake", "tsunami", "landslide", "mudslide",
      "blizzard", "ice storm", "power outage", "severe weather", "flooding"
    ];
    const query = queryTokens.join(" OR ");

    const sources = "cnn,bbc-news,associated-press,reuters,the-weather-channel,abc-news,nbc-news";
    const url = `${NEWS_API_URL}?q=${encodeURIComponent(query)}&sources=${sources}&language=en&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 NewsAPI URL:", url);
    console.log("🔑 API Key present?", !!NEWS_API_KEY);

    const { data } = await axios.get(url, { timeout: 25000 });
    if (!data.articles || !Array.isArray(data.articles)) {
      console.warn("⚠️ No valid articles found");
      return;
    }

    // Normalize and keep only high-confidence results
    const normalized = data.articles
      .map(normalizeArticle)
      .filter(Boolean)
      .filter((a) => (a.geometry?.confidence ?? 0) >= 2); // only state-confirmed or county+state

    console.log(`✅ Parsed ${normalized.length} relevant of ${data.articles.length} total`);
    if (normalized.length) {
      console.log(
        normalized
          .slice(0, 6)
          .map((a) => `🌎 ${a.title} — ${a.source} (${a.geometry.state}, ${a.geometryMethod})`)
          .join("\n")
      );
      await saveNewsArticles(normalized);
    }
  } catch (err) {
    if (err?.response) {
      console.error("❌ NewsAPI error:", err.response.status, err.response.data);
    } else {
      console.error("❌ NewsAPI request failed:", err.message);
    }
  }
}
