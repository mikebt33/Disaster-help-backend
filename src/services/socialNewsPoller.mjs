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
  "avalanche", "hailstorm", "dust storm", "snowstorm", "ice storm",
  "power outage", "blackout", "evacuation", "shelter", "rescue"
];

// 🚫 Non-disaster / foreign / entertainment blocklist ------------------------
const BLOCK_TERMS =
  /(gaza|israel|ukraine|russia|idf|hamas|hezbollah|missile|airstrike|attack|smuggler|drug|cartel|shooting|murder|politic|election|party|military|conflict|war|terror|crime|border patrol|museum|concert|celebrity|swift|movie|award|fashion|sport|soccer|baseball|nba|football|hockey|german|mexico|canada|china|japan|india|africa|europe|paris|london|berlin|madrid|moscow)/i;

// --- Geo logic --------------------------------------------------------------
function tryLocationFromText(text) {
  if (!text) return null;
  const lower = text.toLowerCase();

  // Alaska special case — many alerts reference it explicitly
  if (/\balaska\b|\banchorage\b|\bjuneau\b|\bkenai\b|\bbethel\b|\bnome\b|\bfairbanks\b|\bwasilla\b/.test(lower)) {
    const counties = countyCenters["AK"];
    if (counties) {
      const coordsArray = Object.values(counties).filter(Array.isArray);
      const avgLon = coordsArray.reduce((s, c) => s + c[0], 0) / coordsArray.length;
      const avgLat = coordsArray.reduce((s, c) => s + c[1], 0) / coordsArray.length;
      return {
        type: "Point",
        coordinates: applyJitter([avgLon, avgLat]),
        method: "state",
        state: "AK"
      };
    }
  }

  // Full state name to abbreviation map
  const STATE_ABBR = {
    alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR", california: "CA",
    colorado: "CO", connecticut: "CT", delaware: "DE", florida: "FL", georgia: "GA",
    hawaii: "HI", idaho: "ID", illinois: "IL", indiana: "IN", iowa: "IA", kansas: "KS",
    kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD", massachusetts: "MA",
    michigan: "MI", minnesota: "MN", mississippi: "MS", missouri: "MO", montana: "MT",
    nebraska: "NE", nevada: "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", tennessee: "TN", texas: "TX",
    utah: "UT", vermont: "VT", virginia: "VA", washington: "WA", "west virginia": "WV",
    wisconsin: "WI", wyoming: "WY"
  };

  // 1️⃣ County-level match
  for (const [stateCode, counties] of Object.entries(countyCenters)) {
    for (const [countyName, coords] of Object.entries(counties)) {
      if (Array.isArray(coords) && lower.includes(countyName.toLowerCase())) {
        return {
          type: "Point",
          coordinates: applyJitter(coords),
          method: "county",
          state: stateCode
        };
      }
    }
  }

  // 2️⃣ State-level match
  const stateMatch = Object.keys(STATE_ABBR).find(name => lower.includes(name));
  if (stateMatch) {
    const abbr = STATE_ABBR[stateMatch];
    const counties = countyCenters[abbr];
    if (counties && Object.keys(counties).length) {
      const coordsArray = Object.values(counties).filter(Array.isArray);
      const avgLon = coordsArray.reduce((s, c) => s + c[0], 0) / coordsArray.length;
      const avgLat = coordsArray.reduce((s, c) => s + c[1], 0) / coordsArray.length;
      return {
        type: "Point",
        coordinates: applyJitter([avgLon, avgLat]),
        method: "state",
        state: abbr
      };
    }
  }

  return null;
}

// Add random jitter to avoid identical stacking
function applyJitter([lon, lat]) {
  const jitter = 0.3; // ~25–30 km offset
  const lonOffset = (Math.random() - 0.5) * jitter;
  const latOffset = (Math.random() - 0.5) * jitter;
  return [lon + lonOffset, lat + latOffset];
}

// --- Normalize and filter article ------------------------------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const text = `${title} ${desc}`.toLowerCase();

    // Skip irrelevant topics or foreign references
    if (BLOCK_TERMS.test(text)) return null;

    // Exclude figurative “flood” uses like “fans flood museum”
    if (/fans flood|flooded with applause|sales flood|inbox flooded/.test(text))
      return null;

    // Must contain a core hazard
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
      expires: new Date(Date.now() + 24 * 60 * 60 * 1000) // 24 hours
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
