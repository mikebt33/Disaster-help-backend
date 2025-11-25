// news/socialNewsPoller.mjs
// Loosened disaster-focused U.S.-only NewsAPI poller.
// Still conservative, still clean, but now produces real alerts.

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

// Precompute conservative state centroids
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

// State name ↔ code maps
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
// Loosened hazard keywords — still all real disaster phenomena.
const HAZARD_WORDS = [
  "flash flood", "storm surge", "hurricane", "tropical storm", "tornado",
  "cyclone", "typhoon", "wildfire", "forest fire", "earthquake", "aftershock",
  "tsunami", "volcano", "eruption", "landslide", "mudslide", "blizzard",
  "heat wave", "heatwave", "drought", "avalanche", "hailstorm", "dust storm",
  "severe weather", "snowstorm", "ice storm", "power outage", "blackout",
  "floods", "flooding",

  // newly added broadeners:
  "storm", "damaging winds", "heavy rain", "rainfall", "storm damage",
  "winter storm", "extreme heat", "wind advisory", "weather warning",
  "weather alert"
];

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const HAZARD_RE_SOURCE =
  "\\b(" + HAZARD_WORDS.sort((a, b) => b.length - a.length).map(esc).join("|") + ")\\b";
const makeHazardRe = (flags) => new RegExp(HAZARD_RE_SOURCE, flags);

// Noise blockers (kept)
const HARD_BLOCK =
  /(\b(taylor swift|grammy|oscars?|hollywood|netflix|museum|painting|concert|celebrity|theater|movie|album|music video)\b)|(\b(senate|congress|election|campaign|supreme court|lawsuit|indictment|democrat|republican|white house|trump|biden)\b)|(\b(stock|market|loan|bailout|stimulus|bond|treasury|fiscal|budget)\b)|(\b(shooting|murder|assault|homicide|kidnapping|smuggler|cartel)\b)/i;

// Figurative (kept)
const FIGURATIVE = /\bfans?\s+flood|sales?\s+flood|tweets?\s+flood|inbox\s+flood|orders?\s+flood|applications?\s+flood/i;

// Foreign block — DISABLED for safer U.S. filtering
// const FOREIGN = /\b(... huge list ...)\b/i;

// Context validators (kept)
const CONTEXT_VALIDATORS =
  /\b(storm|rain|rains|rainfall|wind|winds|mph|gusts|surge|inches|river|creek|coast|coastal|shore|evacuations?|shelters?|rescued?|guard|national guard|coast guard|emergency|nws|weather service|forecast|warning|watch|advisory|landfall|damage|inundation|mud|debris|burn|firefighters?)\b/i;

// Weak alone (kept)
const WEAK_ALONE = /\b(rescue|rescues|rescued|evacuation|evacuations|evacuate|evacuated)\b/i;

// Washington handling
const WASHINGTON_FALSE = /\b(washington post|washington,\s*d\.?c\.?|washington dc)\b/i;
const WASHINGTON_STRICT = /\b(washington state|western washington|eastern washington|state of washington)\b/i;

// -------------------- US bounds --------------------
function inUSBounds(lon, lat) {
  const conus = lat >= 24 && lat <= 49.5 && lon >= -125 && lon <= -66.5;
  const alaska = lat >= 51 && lat <= 71.8 && lon >= -179.2 && lon <= -129;
  const hawaii = lat >= 18.8 && lat <= 22.4 && lon >= -160.6 && lon <= -154.4;
  return conus || alaska || hawaii;
}

function jitter(coords, deg = 0.15) {
  const [lon, lat] = coords;
  return [lon + (Math.random() - 0.5) * deg, lat + (Math.random() - 0.5) * deg];
}

// -------------------- State regexes --------------------
function toStateCode(token) {
  if (!token) return null;
  const t = token.trim();
  if (STATE_CODES.has(t.toUpperCase())) return t.toUpperCase();
  const n = t.toLowerCase();
  if (STATE_NAME_TO_CODE[n]) return STATE_NAME_TO_CODE[n];
  return null;
}

const makeStateAbbrRe = () => new RegExp("\\b(" + Array.from(STATE_CODES).join("|") + ")\\b", "g");
const makeStateFullRe = () => new RegExp("\\b(" + Array.from(STATE_NAMES).join("|") + ")\\b", "gi");

// -------------------- Coastal helpers --------------------
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

// -------------------- Location extraction (loosened windows) --------------------
function extractLocation(textRaw) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  const hazardMatches = [...text.matchAll(makeHazardRe("gi"))];
  if (hazardMatches.length === 0) return null;

  // Still block obvious noise
  if (HARD_BLOCK.test(text) || FIGURATIVE.test(text)) return null;

  // NOTE: FOREIGN block disabled

  // Validate real hazard context
  let hasValidatedHazard = false;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 300);
    const end = Math.min(text.length, m.index + m[0].length + 300);
    const window = text.slice(start, end);

    if (CONTEXT_VALIDATORS.test(window) || !WEAK_ALONE.test(window)) {
      hasValidatedHazard = true;
      break;
    }
  }

  if (!hasValidatedHazard) return null;

  // AK and HI explicit
  if (/\balaska\b/.test(text) && STATE_CENTROIDS.AK) {
    return { type: "Point", coordinates: jitter(STATE_CENTROIDS.AK), method: "state", state: "AK", confidence: 3 };
  }
  if (/\bhawaii\b/.test(text) && STATE_CENTROIDS.HI) {
    return { type: "Point", coordinates: jitter(STATE_CENTROIDS.HI), method: "state", state: "HI", confidence: 3 };
  }

  // County, ST
  const countyRe = /([A-Za-z.\- ']+?)\s+county(?:\s+[\w'.-]+)?\s*,\s*([A-Za-z ]{2,})\b/gi;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 300);
    const end = Math.min(text.length, m.index + m[0].length + 300);
    const window = text.slice(start, end);

    let cm;
    while ((cm = countyRe.exec(window))) {
      const countyName = cm[1].trim().replace(/\s+Sheriff.*$/i, "");
      const stCode = toStateCode(cm[2]);
      if (stCode && countyCenters[stCode] && countyCenters[stCode][countyName]) {
        const coords = countyCenters[stCode][countyName];
        return { type: "Point", coordinates: jitter(coords), method: "county", state: stCode, confidence: 3 };
      }
    }
  }

  // Datelines
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 300);
    const end = Math.min(text.length, m.index + m[0].length + 300);
    const window = text.slice(start, end);

    const datelineRe =
      /[,–—-]\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|[A-Za-z ]{4,})\b/gi;

    let d;
    while ((d = datelineRe.exec(window))) {
      let token = d[1].trim();
      if (/^washington$/i.test(token) && !WASHINGTON_STRICT.test(window)) continue;
      const stCode = toStateCode(token);
      if (stCode && STATE_CENTROIDS[stCode]) {
        return { type: "Point", coordinates: jitter(STATE_CENTROIDS[stCode]), method: "state", state: stCode, confidence: 2 };
      }
    }
  }

  // Coastal heuristics (kept)
  const mentionsHurricane = /(hurricane|tropical storm|storm surge)/i.test(text);

  if (mentionsHurricane && GULF_HINT.test(text)) {
    const picked = pickFromMentioned(text, GULF_STATES) || "TX";
    if (STATE_CENTROIDS[picked]) {
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[picked], 0.2), method: "coastal-heuristic", state: picked, confidence: 2 };
    }
  }

  if (mentionsHurricane && ATLANTIC_HINT.test(text)) {
    const picked = pickFromMentioned(text, ATLANTIC_STATES) || "FL";
    if (picked === "FL") {
      return { type: "Point", coordinates: [-80.5, 27.5], method: "state-coastal-bias", state: "FL", confidence: 3 };
    }
    if (STATE_CENTROIDS[picked]) {
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[picked], 0.2), method: "coastal-heuristic", state: picked, confidence: 2 };
    }
  }

  // Simple state inference
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 300);
    const end = Math.min(text.length, m.index + m[0].length + 300);
    const window = text.slice(start, end);

    const abbrRe = makeStateAbbrRe();
    const fullRe = makeStateFullRe();
    const simpleStateTokens = [
      ...window.matchAll(abbrRe),
      ...window.matchAll(fullRe)
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
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[candidate[0]]), method: "state", state: candidate[0], confidence: 1 };
    }
  }

  return null;
}

// -------------------- Normalize article --------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const content = (article.content || "").trim();
    const text = `${title}\n${desc}\n${content}`;

    if (!title || /taylor swift|election|hollywood/i.test(text))
      return null;

    let geometry = extractLocation(text);

    // OPTIONAL: fallback — try title alone
    if (!geometry && title.length > 0) {
      geometry = extractLocation(title);
    }

    if (!geometry) return null;

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
      createdAt: new Date()
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

    for (const a of articles) {
      if (!a.url) continue;
      await col.updateOne(
        { url: a.url },
        { $set: { ...a, createdAt: new Date() } },
        { upsert: true }
      );
    }

    console.log(`💾 Saved ${articles.length} news articles`);
  } catch (err) {
    console.error("❌ Error saving news articles:", err.message);
  }
}

// -------------------- Poller --------------------
export async function pollNewsAPI() {
  console.log("📰 News Poller running…");
  try {
    const queryTokens = [
      "flash flood", "storm surge", "hurricane", "tropical storm",
      "tornado", "wildfire", "earthquake", "tsunami", "landslide", "mudslide",
      "blizzard", "ice storm", "power outage", "severe weather", "flooding",

      // new broadeners
      "storm", "damaging winds", "heavy rain", "rainfall",
      "storm damage", "winter storm", "extreme heat"
    ];

    const query = queryTokens.join(" OR ");

    // 🔥 Removed source restriction — allows local/regional reporting
    const sources = "";

    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
    const to = now.toISOString();

    const url =
      `${NEWS_API_URL}?q=${encodeURIComponent(query)}` +
      `&language=en&from=${from}&to=${to}&sortBy=publishedAt&pageSize=50` +
      `&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 NewsAPI URL:", url);

    const { data } = await axios.get(url, { timeout: 25000 });
    if (!data.articles || !Array.isArray(data.articles)) {
      console.warn("⚠️ No valid articles returned");
      return;
    }

    const normalized = data.articles
      .map(normalizeArticle)
      .filter(Boolean)
      .filter((a) => (a.geometry?.confidence ?? 0) >= 1);

    console.log(`✅ Parsed ${normalized.length} of ${data.articles.length}`);

    if (normalized.length) {
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
