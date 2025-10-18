import axios from "axios";
import { getDB } from "../db.js";
import { XMLParser } from "fast-xml-parser"; // only if you reuse XML logic
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// Simple geo helpers from your CAP setup
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

/**
 * Social/News Poller — MVP
 * Fetches recent disaster-related news, geocodes based on county/state mentions,
 * and saves to Mongo for short-term display (TTL = 2h).
 */

const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY; // ← set this in Render dashboard

// Disaster-related keywords
const KEYWORDS = [
  "flood", "hurricane", "tornado", "earthquake", "wildfire",
  "evacuation", "storm", "emergency", "disaster", "rescue",
  "power outage", "mudslide", "landslide", "flash flood"
];

// Basic geocoding from county/state mention
function tryLocationFromText(text) {
  if (!text) return null;
  const stateMatch = text.match(/\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR|GU|AS|MP|VI)\b/);
  const state = stateMatch?.[1];
  if (state && countyCenters[state]) {
    return { type: "Point", coordinates: countyCenters[state].__center || [0, 0] };
  }
  return null;
}

// Normalize and clean one article
function normalizeArticle(article) {
  try {
    const text = `${article.title || ""} ${article.description || ""}`;
    const lower = text.toLowerCase();

    if (!KEYWORDS.some(k => lower.includes(k))) return null;

    const geometry = tryLocationFromText(text);
    if (!geometry) return null;

    return {
      title: article.title || "News Update",
      description: article.description || "",
      url: article.url || "",
      source: article.source?.name || "Unknown",
      publishedAt: new Date(article.publishedAt || Date.now()),
      geometry,
      geometryMethod: "newsapi-geo",
      timestamp: new Date(),
      expires: new Date(Date.now() + 2 * 60 * 60 * 1000), // 2h TTL
    };
  } catch (err) {
    console.warn("❌ Error normalizing article:", err.message);
    return null;
  }
}

// Save to Mongo
async function saveNewsArticles(articles) {
  try {
    const db = getDB();
    const collection = db.collection("social_signals");

    // TTL cleanup: remove expired
    await collection.deleteMany({ expires: { $lte: new Date() } });

    for (const article of articles) {
      await collection.updateOne(
        { url: article.url },
        { $set: article },
        { upsert: true }
      );
    }

    console.log(`💾 Saved ${articles.length} news articles`);
  } catch (err) {
    console.error("❌ Error saving news articles:", err.message);
  }
}

// Fetch & process
async function pollNewsAPI() {
  console.log("📰 News Poller running...");
  try {
    const query = KEYWORDS.join(" OR ");
    const url = `${NEWS_API_URL}?q=${encodeURIComponent(query)}&language=en&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;
    const { data } = await axios.get(url, { timeout: 20000 });

    if (!data.articles || !Array.isArray(data.articles)) {
      console.warn("⚠️ No valid articles found");
      return;
    }

    const normalized = data.articles.map(normalizeArticle).filter(Boolean);
    console.log(`✅ Parsed ${normalized.length} relevant articles`);
    if (normalized.length) await saveNewsArticles(normalized);
  } catch (err) {
    console.error("❌ Error polling NewsAPI:", err.message);
  }
}

export { pollNewsAPI };
