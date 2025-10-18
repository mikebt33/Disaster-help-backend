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

// ✅ Core U.S. hazard keywords -----------------------------------------------
const CORE_HAZARDS = [
  "flood", "flash flood", "storm surge", "hurricane", "tropical storm",
  "tornado", "cyclone", "typhoon", "wildfire", "bushfire", "forest fire",
  "earthquake", "aftershock", "tsunami", "volcano", "eruption",
  "landslide", "mudslide", "blizzard", "heatwave", "drought",
  "avalanche", "hailstorm", "dust storm", "severe weather",
  "snowstorm", "ice storm", "power outage", "blackout",
  "evacuation", "evacuations", "shelter", "rescue"
];

// 🚫 Non-disaster / foreign / political blocklist ----------------------------
const BLOCK_TERMS = /(gaza|israel|ukraine|russia|idf|hamas|hezbollah|missile|airstrike|attack|smuggler|drug|cartel|shooting|murder|politic|election|redistrict|party|military|conflict|war|terror|crime|border patrol)/i;

// --- Geo logic --------------------------------------------------------------
function tryLocationFromText(text) {
  if (!text) return null;
  const lower = text.toLowerCase();

  // --- 1️⃣ Detect explicit state name or abbreviation -----------------------
  const stateMatch = lower.match(
    /\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)\b|(\bAL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/g
  );

  let matchedState = null;
  if (stateMatch && stateMatch[0]) {
    matchedState = stateMatch[0]
      .toUpperCase()
      .replace(/[^A-Z]/g, "")
      .slice(0, 2);
  }

  // --- 2️⃣ If a state is found, check for a county inside that same state ----
  if (matchedState && countyCenters[matchedState]) {
    const counties = countyCenters[matchedState];
    for (const [countyName, coords] of Object.entries(counties)) {
      if (Array.isArray(coords) && lower.includes(countyName.toLowerCase())) {
        return {
          type: "Point",
          coordinates: coords,
          method: "county",
          state: matchedState
        };
      }
    }

    // No county match; fall back to state centroid
    const coordsArray = Object.values(counties).filter(Array.isArray);
    const avgLon = coordsArray.reduce((sum, c) => sum + c[0], 0) / coordsArray.length;
    const avgLat = coordsArray.reduce((sum, c) => sum + c[1], 0) / coordsArray.length;

    return {
      type: "Point",
      coordinates: [avgLon, avgLat],
      method: "state",
      state: matchedState
    };
  }

  // --- 3️⃣ If no state, skip entirely ---------------------------------------
  return null;
}

// --- Normalize and filter article ------------------------------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const text = `${title} ${desc}`.toLowerCase();

    // Skip irrelevant topics
    if (BLOCK_TERMS.test(text)) return null;

    // Must contain at least one core hazard
    const hasHazard = CORE_HAZARDS.some(k => text.includes(k));
    if (!hasHazard) return null;

    // Must geolocate to a U.S. state
    const geometry = tryLocationFromText(text);
    if (!geometry) return null;

    // Extract domain
    let domain = "";
    try {
      const u = new URL(article.url);
      domain = u.hostname.replace("www.", "");
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
      expires: new Date(Date.now() + 2 * 60 * 60 * 1000) // 2 hours
    };
  } catch (err) {
    console.warn("❌ Error normalizing article:", err.message);
    return null;
  }
}

// --- Save to Mongo ---------------------------------------------------------
async function saveNewsArticles(articles) {
  try {
    const db = getDB();
    const collection = db.collection("social_signals");

    // Clean expired
    await collection.deleteMany({ expires: { $lte: new Date() } });

    for (const a of articles) {
      await collection.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    }

    console.log(`💾 Saved ${articles.length} news articles`);
  } catch (err) {
    console.error("❌ Error saving news articles:", err.message);
  }
}

// --- Poller main -----------------------------------------------------------
async function pollNewsAPI() {
  console.log("📰 News Poller running (strict U.S. disaster focus, correct-state mapping)...");
  try {
    const sample = CORE_HAZARDS.sort(() => 0.5 - Math.random()).slice(0, 10);
    const query = sample.join(" OR ");
    const sources =
      "cnn,bbc-news,associated-press,reuters,the-weather-channel,abc-news,nbc-news";

    const url = `${NEWS_API_URL}?q=${encodeURIComponent(
      query
    )}&sources=${sources}&language=en&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;

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
          .map(
            a =>
              `🌎 ${a.title} — ${a.source} (${a.geometry.state || "?"}, ${a.geometryMethod})`
          )
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
