import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

console.log("NEWS_API_KEY loaded:", !!process.env.NEWS_API_KEY);

// --- Geo helpers ------------------------------------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

// --- Config -----------------------------------------------------------------
const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

// 🧭 Phase 1: Broad discovery keyword model
const KEYWORDS = [
  // Core hazards
  "flood", "flash flood", "storm surge", "hurricane", "tropical storm",
  "tornado", "cyclone", "typhoon", "wildfire", "bushfire", "forest fire",
  "earthquake", "aftershock", "tsunami", "volcano", "eruption",
  "landslide", "mudslide", "blizzard", "heatwave", "drought",
  "avalanche", "hailstorm", "dust storm", "severe weather",
  // Impacts and response
  "evacuation", "shelter", "rescue", "emergency", "disaster",
  "power outage", "blackout", "road closure", "infrastructure damage",
  "relief effort", "aid agency", "civil defense", "FEMA", "Red Cross"
];

// --- Helper: derive geo point from text -------------------------------------
function tryLocationFromText(text) {
  if (!text) return null;
  const lower = text.toLowerCase();

  // 1️⃣ County-level search
  for (const [stateCode, meta] of Object.entries(countyCenters)) {
    const counties = meta.counties || {};
    for (const [countyName, cData] of Object.entries(counties)) {
      if (lower.includes(countyName.toLowerCase())) {
        return { type: "Point", coordinates: cData.center || [0, 0], method: "county" };
      }
    }
  }

  // 2️⃣ State abbreviation or full name
  const stateMatch = lower.match(
    /\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)\b|(\bAL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/g
  );
  if (stateMatch && stateMatch[0]) {
    const foundState = stateMatch[0].toUpperCase().slice(0, 2);
    if (countyCenters[foundState]) {
      return { type: "Point", coordinates: countyCenters[foundState].__center || [0, 0], method: "state" };
    }
  }

  // 3️⃣ Fallback to U.S. centroid
  return { type: "Point", coordinates: [-98.5795, 39.8283], method: "us-centroid" };
}

// --- Normalize and filter article -------------------------------------------
function normalizeArticle(article) {
  try {
    const text = `${article.title || ""} ${article.description || ""}`;
    const lower = text.toLowerCase();

    // Allow looser matching — at least one keyword anywhere in title or description
    const matches = KEYWORDS.filter(k => lower.includes(k));
    if (matches.length === 0) {
      // no direct keyword hit, but maybe it’s disaster-related by source?
      const disasterish = /(storm|flood|earthquake|fire|disaster|emergency|evacuat|outage)/i;
      if (!disasterish.test(text)) return null;
    }

    const geometry = tryLocationFromText(text);
    if (!geometry) return null;

    let domain = "";
    try {
      const u = new URL(article.url);
      domain = u.hostname.replace("www.", "");
    } catch (_) {}

    return {
      title: article.title || "News Update",
      description: article.description || "",
      url: article.url || "",
      source: article.source?.name || domain || "Unknown",
      domain,
      publishedAt: new Date(article.publishedAt || Date.now()),
      geometry,
      geometryMethod: geometry.method || "newsapi-geo",
      timestamp: new Date(),
      expires: new Date(Date.now() + 2 * 60 * 60 * 1000)
    };
  } catch (err) {
    console.warn("❌ Error normalizing article:", err.message);
    return null;
  }
}

// --- Save to Mongo ----------------------------------------------------------
async function saveNewsArticles(articles) {
  try {
    const db = getDB();
    const collection = db.collection("social_signals");
    await collection.deleteMany({ expires: { $lte: new Date() } });

    for (const a of articles) {
      await collection.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    }
    console.log(`💾 Saved ${articles.length} news articles`);
  } catch (err) {
    console.error("❌ Error saving news articles:", err.message);
  }
}

// --- Poller main ------------------------------------------------------------
async function pollNewsAPI() {
  console.log("📰 News Poller running (broad discovery + county→state→US geo)...");
  try {
    const sample = KEYWORDS.sort(() => 0.5 - Math.random()).slice(0, 12);
    const query = sample.join(" OR ");
    // Broaden by focusing on disaster-relevant outlets
    const disasterSources = "cnn,bbc-news,associated-press,reuters,the-weather-channel,abc-news,nbc-news";
    const url = `${NEWS_API_URL}?q=${encodeURIComponent(query)}&sources=${disasterSources}&language=en&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 NewsAPI URL:", url);
    console.log("🔑 API Key present?", !!NEWS_API_KEY);

    const { data } = await axios.get(url, { timeout: 25000 });
    if (!data.articles || !Array.isArray(data.articles)) {
      console.warn("⚠️ No valid articles found");
      return;
    }

    const normalized = data.articles.map(normalizeArticle).filter(Boolean);
    console.log(`✅ Parsed ${normalized.length} relevant of ${data.articles.length} total`);
    if (normalized.length) {
      console.log(
        normalized
          .slice(0, 5)
          .map(a => `🌎 ${a.title} — ${a.source} (${a.geometryMethod})`)
          .join("\n")
      );
      await saveNewsArticles(normalized);
    }
  } catch (err) {
    if (err.response) {
      console.error("❌ NewsAPI error:", err.response.status, err.response.data);
    } else {
      console.error("❌ NewsAPI request failed:", err.message);
    }
  }
}

export { pollNewsAPI };
