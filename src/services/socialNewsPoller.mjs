// news/socialNewsPoller.mjs
// Strict U.S.-only disaster poller with high-precision state mapping, coastal heuristics, AK/HI handling, and jittered pins.

import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

console.log("NEWS_API_KEY loaded:", !!process.env.NEWS_API_KEY);

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
const HAZARD_WORDS = [
  "flash flood", "storm surge", "hurricane", "tropical storm",
  "tornado", "wildfire", "earthquake", "tsunami", "landslide", "mudslide",
  "blizzard", "ice storm", "power outage", "severe weather", "flooding"
];
const HAZARD_REGEX = new RegExp(
  "\\b(" +
    HAZARD_WORDS.sort((a, b) => b.length - a.length)
      .map((p) => p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join("|") +
  ")\\b", "gi"
);

const HARD_BLOCK = /(\b(taylor swift|museum|concert|celebrity|hollywood|movie|showbiz|oscars?|music video)\b)|(\b(election|campaign|senate|congress|trump|biden|policy|budget|treasury|loan|stock|market|finance)\b)|(\b(cartel|drug|crime|shooting|murder|kidnap|war|ukraine|israel|gaza)\b)/i;
const FIGURATIVE = /\bfans?\s+flood|tweets?\s+flood|sales?\s+flood|orders?\s+flood/i;
const FOREIGN = /\b(puerto rico|mexico|canada|caribbean|philippines?|argentina|brazil|europe|asia|africa|china|india|russia|ukraine|israel|iran|iraq|australia|france|england|spain|italy)\b/i;
const CONTEXT_VALIDATORS = /\b(storm|rain|winds?|mph|gusts|surge|river|coast|evacuation|rescue|forecast|damage|inundation|debris|firefighters?|weather service|landfall|warning|advisory)\b/i;
const WEAK_ALONE = /\b(rescue|rescues|evacuation|evacuate|evacuated)\b/i;

// -------------------- US bounds --------------------
function inUSBounds(lon, lat) {
  const conus = lat >= 24 && lat <= 49.5 && lon >= -125 && lon <= -66.5;
  const alaska = lat >= 51 && lat <= 71.8 && lon >= -179.2 && lon <= -129;
  const hawaii = lat >= 18.8 && lat <= 22.4 && lon >= -160.6 && lon <= -154.4;
  return conus || alaska || hawaii;
}

// Add jitter for pin stacking
function jitter(coords, deg = 0.2) {
  const [lon, lat] = coords;
  return [
    lon + (Math.random() - 0.5) * deg,
    lat + (Math.random() - 0.5) * deg,
  ];
}

// -------------------- Location extraction --------------------
function toStateCode(token) {
  if (!token) return null;
  const t = token.trim();
  if (STATE_CODES.has(t.toUpperCase())) return t.toUpperCase();
  const n = t.toLowerCase();
  if (STATE_NAME_TO_CODE[n]) return STATE_NAME_TO_CODE[n];
  return null;
}

function extractLocation(textRaw) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  // Require hazard + validating context
  const hazardMatches = [...text.matchAll(HAZARD_REGEX)];
  if (hazardMatches.length === 0) return null;
  if (HARD_BLOCK.test(text) || FOREIGN.test(text) || FIGURATIVE.test(text)) return null;

  let hasContext = false;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 100);
    const end = Math.min(text.length, m.index + m[0].length + 100);
    const window = text.slice(start, end);
    if (CONTEXT_VALIDATORS.test(window) || !WEAK_ALONE.test(window)) {
      hasContext = true;
      break;
    }
  }
  if (!hasContext) return null;

  // Alaska/Hawaii direct match
  if (/\balaska\b/.test(text)) return { type: "Point", coordinates: STATE_CENTROIDS.AK, method: "state", state: "AK", confidence: 3 };
  if (/\bhawaii\b/.test(text)) return { type: "Point", coordinates: STATE_CENTROIDS.HI, method: "state", state: "HI", confidence: 3 };

  // Coastal hurricane heuristic
  if (/(atlantic|bermuda|caribbean)/i.test(text) && /(carolina|florida|georgia|virginia)/i.test(text)) {
    return { type: "Point", coordinates: jitter(STATE_CENTROIDS.FL), method: "coastal-heuristic", state: "FL", confidence: 2 };
  }

  // County + state pair
  const countyRe = /([A-Za-z.\- ']+?)\s+county,\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)/gi;
  let cm;
  while ((cm = countyRe.exec(text))) {
    const countyName = cm[1].trim();
    const st = toStateCode(cm[2]);
    if (st && countyCenters[st] && countyCenters[st][countyName]) {
      const coords = countyCenters[st][countyName];
      if (Array.isArray(coords) && inUSBounds(coords[0], coords[1])) {
        return { type: "Point", coordinates: jitter(coords, 0.1), method: "county", state: st, confidence: 3 };
      }
    }
  }

  // State only
  const stateMatch = text.match(/\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)\b/);
  if (stateMatch) {
    const st = toStateCode(stateMatch[1]);
    if (st && STATE_CENTROIDS[st]) {
      return { type: "Point", coordinates: jitter(STATE_CENTROIDS[st], 0.2), method: "state", state: st, confidence: 2 };
    }
  }

  return null;
}

// -------------------- Normalize & filter article --------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const content = (article.content || "").trim();
    const text = `${title}\n${desc}\n${content}`;

    if (HARD_BLOCK.test(text) || FOREIGN.test(text) || FIGURATIVE.test(text)) return null;
    if (!HAZARD_REGEX.test(text)) return null;

    const geometry = extractLocation(text);
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
      timestamp: new Date(),
      expires: new Date(Date.now() + 24 * 60 * 60 * 1000)
    };
  } catch (err) {
    console.warn("❌ normalizeArticle error:", err.message);
    return null;
  }
}

// -------------------- Save to Mongo --------------------
async function saveNewsArticles(articles) {
  try {
    const db = getDB();
    const col = db.collection("social_signals");
    await col.deleteMany({ expires: { $lte: new Date() } });

    for (const a of articles) {
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
  console.log("📰 News Poller running (strict U.S. disaster focus; conservative+accurate)…");
  try {
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

    const normalized = data.articles
      .map(normalizeArticle)
      .filter(Boolean)
      .filter((a) => (a.geometry?.confidence ?? 0) >= 2);

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
