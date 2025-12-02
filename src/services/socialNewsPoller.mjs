// news/socialNewsPoller.mjs
// Disaster-focused U.S.-only NewsAPI poller.
// Much more permissive, reliable, and location-aware.

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

// Precompute state centroids
const STATE_CENTROIDS = {};
for (const [st, counties] of Object.entries(countyCenters)) {
  const coords = Object.values(counties).filter((v) => Array.isArray(v));
  if (coords.length) {
    STATE_CENTROIDS[st] = [
      coords.reduce((s, c) => s + c[0], 0) / coords.length,
      coords.reduce((s, c) => s + c[1], 0) / coords.length
    ];
  }
}

// -------------------- Config --------------------
const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

// US state maps
const STATE_NAME_TO_CODE = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR",
  california: "CA", colorado: "CO", connecticut: "CT", delaware: "DE",
  florida: "FL", georgia: "GA", hawaii: "HI", idaho: "ID", illinois: "IL",
  indiana: "IN", iowa: "IA", kansas: "KS", kentucky: "KY", louisiana: "LA",
  maine: "ME", maryland: "MD", massachusetts: "MA", michigan: "MI",
  minnesota: "MN", mississippi: "MS", missouri: "MO", montana: "MT",
  nebraska: "NE", nevada: "NV", "new hampshire": "NH", "new jersey": "NJ",
  "new mexico": "NM", "new york": "NY", "north carolina": "NC",
  "north dakota": "ND", ohio: "OH", oklahoma: "OK", oregon: "OR",
  pennsylvania: "PA", "rhode island": "RI", "south carolina": "SC",
  "south dakota": "SD", tennessee: "TN", texas: "TX", utah: "UT",
  vermont: "VT", virginia: "VA", washington: "WA", "west virginia": "WV",
  wisconsin: "WI", wyoming: "WY"
};
const STATE_CODES = new Set(Object.values(STATE_NAME_TO_CODE));
const STATE_NAMES = new Set(Object.keys(STATE_NAME_TO_CODE));

// -------------------- Hazard keywords --------------------
const HAZARD_WORDS = [
  "flood", "flash flood", "storm", "severe weather", "tornado",
  "hurricane", "tropical storm", "wildfire", "forest fire",
  "earthquake", "tsunami", "landslide", "mudslide", "blizzard",
  "winter storm", "power outage"
];

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const HAZARD_RE = new RegExp(
  "\\b(" +
    HAZARD_WORDS.sort((a, b) => b.length - a.length).map(esc).join("|") +
    ")\\b",
  "gi"
);

// -------------------- Noise blockers --------------------
// REMOVED "hollywood" so LA storms are not incorrectly filtered.
const HARD_BLOCK =
  /(\b(taylor swift|grammy|oscars?|netflix|museum|painting|concert|celebrity|theater|movie|album|music video)\b)|(\b(senate|congress|election|campaign|supreme court|lawsuit|indictment|democrat|republican|white house|trump|biden)\b)|(\b(stock|market|loan|bailout|stimulus|bond|treasury|fiscal|budget)\b)|(\b(shooting|murder|assault|homicide|kidnapping|smuggler|cartel)\b)/i;

const FIGURATIVE = /\bfans?\s+flood|sales?\s+flood|tweets?\s+flood/i;

const CONTEXT_VALIDATORS =
  /\b(storm|rain|wind|mph|gust|coast|warning|watch|advisory|flood|river|creek|nws|weather service|emergency|landfall|damage|inundation|mud|debris|burn|firefighters?)\b/i;

// -------------------- Helpers --------------------
function jitter(coords, deg = 0.15) {
  const [lon, lat] = coords;
  return [
    lon + (Math.random() - 0.5) * deg,
    lat + (Math.random() - 0.5) * deg
  ];
}

function toStateCode(token) {
  if (!token) return null;
  const t = token.trim();
  if (STATE_CODES.has(t.toUpperCase())) return t.toUpperCase();
  const lc = t.toLowerCase();
  return STATE_NAME_TO_CODE[lc] || null;
}

// -------------------- City, State extractor --------------------
function tryCityState(text) {
  const re =
    /([A-Za-z.\- ']+?),\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/gi;

  let m;
  while ((m = re.exec(text))) {
    const st = m[2].toUpperCase();
    if (STATE_CENTROIDS[st]) {
      return {
        type: "Point",
        coordinates: jitter(STATE_CENTROIDS[st]),
        method: "city-state",
        state: st,
        confidence: 2
      };
    }
  }
  return null;
}

// -------------------- State fallback --------------------
function fallbackState(text) {
  for (const name of STATE_NAMES) {
    if (text.includes(name)) {
      const st = STATE_NAME_TO_CODE[name];
      if (st && STATE_CENTROIDS[st]) {
        return {
          type: "Point",
          coordinates: jitter(STATE_CENTROIDS[st]),
          method: "state-fallback",
          state: st,
          confidence: 1
        };
      }
    }
  }
  return null;
}

// -------------------- Extract Location --------------------
function extractLocation(textRaw) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  const hazardMatches = [...text.matchAll(HAZARD_RE)];
  if (hazardMatches.length === 0) return null;

  if (HARD_BLOCK.test(text) || FIGURATIVE.test(text)) return null;

  // Loosened: just require real hazard context
  let confirmed = false;
  for (const m of hazardMatches) {
    const start = Math.max(0, m.index - 600);
    const end = Math.min(text.length, m.index + m[0].length + 600);
    if (CONTEXT_VALIDATORS.test(text.slice(start, end))) {
      confirmed = true;
      break;
    }
  }
  if (!confirmed) return null;

  // City, ST pattern
  const city = tryCityState(text);
  if (city) return city;

  // State fallback
  const st = fallbackState(text);
  if (st) return st;

  return null;
}

// -------------------- Normalize article --------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const content = (article.content || "").trim();
    const text = `${title}\n${desc}\n${content}`.toLowerCase();

    if (!title) return null;

    let geometry = extractLocation(text);
    if (!geometry) geometry = extractLocation(title.toLowerCase());
    if (!geometry) return null;

    let domain = "";
    try {
      domain = new URL(article.url).hostname.replace(/^www\./, "");
    } catch (_) {}

    return {
      title,
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
    console.warn("❌ Error normalizing:", err.message);
    return null;
  }
}

// -------------------- Save --------------------
async function saveNewsArticles(articles) {
  const db = getDB();
  const col = db.collection("social_signals");

  for (const a of articles) {
    if (!a.url) continue;
    await col.updateOne({ url: a.url }, { $set: a }, { upsert: true });
  }

  console.log(`💾 Saved ${articles.length} news articles`);
}

// -------------------- Poller --------------------
export async function pollNewsAPI() {
  console.log("📰 NewsAPI poll running…");

  try {
    const hazardQuery = HAZARD_WORDS.join(" OR ");

    const now = new Date();
    const from = new Date(now.getTime() - 24 * 3600 * 1000).toISOString();
    const to = now.toISOString();

   const sources = "the-weather-channel,associated-press,abc-news,nbc-news,fox-news,npr,bloomberg,usa-today";

   const url =
     `${NEWS_API_URL}?q=${encodeURIComponent(hazardQuery)}` +
     `&sources=${sources}` +
     `&language=en&from=${from}&to=${to}&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 URL:", url);

    const { data } = await axios.get(url, { timeout: 20000 });
    if (!data.articles?.length) {
      console.log("⚠️ No NewsAPI articles returned");
      return;
    }

    const normalized = data.articles
      .map(normalizeArticle)
      .filter(Boolean);

    console.log(`✅ Parsed ${normalized.length} of ${data.articles.length}`);

    if (normalized.length) await saveNewsArticles(normalized);
  } catch (err) {
    console.error("❌ NewsAPI error:", err.message);
  }
}
