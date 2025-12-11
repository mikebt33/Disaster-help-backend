// src/services/socialNewsPoller.mjs
// ------------------------------------------------------------
// SOCIAL NEWS POLLER — MAXIMUM SIGNALS MODE (OPTION A)
// ------------------------------------------------------------
// This is your high-volume hazard intelligence pipeline:
//  • Accepts broad, regional, and statewide hazard articles
//  • Only rejects entertainment / obvious junk
//  • Strong hazard detection with expanded keyword mappings
//  • Region + state + county fallbacks for maximum coverage
//  • Deterministic jitter → no stacking
//  • Designed to ALWAYS produce news signals for your map
//
// Outputs documents into: social_signals
// { type:"news", provider:"NewsAPI", geometry:{Point}, expires, ... }
//
// ------------------------------------------------------------

import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

console.log("NEWS_API_KEY loaded:", !!NEWS_API_KEY);

// ----------------------------------------------------
// Load county centers
// ----------------------------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

// ----------------------------------------------------
// Build state centroids
// ----------------------------------------------------
const STATE_CENTROIDS = {};
for (const [st, counties] of Object.entries(countyCenters)) {
  const values = Object.values(counties).filter((v) => Array.isArray(v));
  if (!values.length) continue;
  STATE_CENTROIDS[st] = [
    values.reduce((s, c) => s + c[0], 0) / values.length,
    values.reduce((s, c) => s + c[1], 0) / values.length,
  ];
}

// ----------------------------------------------------
// Regions (broad fallback for maximum signals)
// ----------------------------------------------------
const REGIONAL_CENTROIDS = {
  "midwest": [-93.5, 42.1],
  "great lakes": [-85.5, 44.0],
  "great plains": [-101.0, 44.0],
  "northern plains": [-102.3, 47.0],
  "southern plains": [-98.0, 33.0],
  "deep south": [-88.0, 32.0],
  "southeast": [-82.7, 33.2],
  "gulf coast": [-90.0, 29.0],
  "northeast": [-72.0, 42.7],
  "mid atlantic": [-76.0, 39.0],
  "pacific northwest": [-121.0, 45.5],
  "southwest": [-111.5, 34.0],
  "rockies": [-108.0, 42.0],
  "california": [-119.3, 36.6],
  "texas": [-99.1, 31.0],
  "florida": [-82.5, 27.5],
};

// ----------------------------------------------------
// State name → code
// ----------------------------------------------------
const STATE_NAME_TO_CODE = {
  alabama:"AL", alaska:"AK", arizona:"AZ", arkansas:"AR",
  california:"CA", colorado:"CO", connecticut:"CT", delaware:"DE",
  florida:"FL", georgia:"GA", hawaii:"HI", idaho:"ID", illinois:"IL",
  indiana:"IN", iowa:"IA", kansas:"KS", kentucky:"KY", louisiana:"LA",
  maine:"ME", maryland:"MD", massachusetts:"MA", michigan:"MI",
  minnesota:"MN", mississippi:"MS", missouri:"MO", montana:"MT",
  nebraska:"NE", nevada:"NV", "new hampshire":"NH", "new jersey":"NJ",
  "new mexico":"NM", "new york":"NY", "north carolina":"NC",
  "north dakota":"ND", ohio:"OH", oklahoma:"OK", oregon:"OR",
  pennsylvania:"PA", "rhode island":"RI", "south carolina":"SC",
  "south dakota":"SD", tennessee:"TN", texas:"TX", utah:"UT",
  vermont:"VT", virginia:"VA", washington:"WA", "west virginia":"WV",
  wisconsin:"WI", wyoming:"WY", "district of columbia":"DC",
};

// ----------------------------------------------------
// Hazard dictionary (maximum coverage)
// ----------------------------------------------------
const HAZARD_WORDS = [
  "tornado", "twister", "funnel cloud",
  "hurricane", "tropical storm", "cyclone",
  "flash flood", "flood", "inundation",
  "severe weather", "damaging winds", "strong winds",
  "thunderstorm", "hail", "microburst", "downburst",
  "wildfire", "forest fire", "brush fire",
  "earthquake", "aftershock",
  "tsunami",
  "landslide", "mudslide",
  "blizzard", "winter storm", "ice storm",
  "power outage", "blackout",
  "extreme heat", "heat wave",
];

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const HAZARD_RE = new RegExp(
  "(" + HAZARD_WORDS.sort((a,b)=>b.length-a.length).map(esc).join("|") + ")",
  "i"
);

// ----------------------------------------------------
// Noise blockers — very lightweight, because OPTION A
// wants maximum throughput
// ----------------------------------------------------
const HARD_BLOCK =
  /\b(taylor swift|concert|movie|album|celebrity|fashion|netflix|trailer|box office)\b/i;

const FIGURATIVE =
  /\b(fans? flood|flood of|media storm|political storm|stormed the)\b/i;

// ----------------------------------------------------
// Context validators (loose for OPTION A)
// ----------------------------------------------------
const CONTEXT_RE =
  /\b(national weather service|nws|evac|damage|destroyed|collapsed|injured|killed|winds?|mph|gust|rain|snow|hail|firefighters?|burned|evacuations?)\b/i;

// ----------------------------------------------------
// Deterministic jitter
// ----------------------------------------------------
function hash32(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function mulberry32(seed) {
  return function() {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), 1 | t);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function jitter([lon, lat], seed, deg = 0.22) {
  const r = mulberry32(hash32(seed));
  return [
    lon + (r() - 0.5) * deg,
    lat + (r() - 0.5) * deg,
  ];
}

// ----------------------------------------------------
// County matching
// ----------------------------------------------------
function normalizeCountyName(raw) {
  if (!raw) return "";
  let s = raw.trim().toLowerCase();
  s = s.replace(/^city of\s+/i, "");
  s = s.replace(/\s+(county|parish)\b/i, "");
  s = s.replace(/^st[.\s]+/i, "saint ");
  return s.replace(/\./g, "").trim();
}

function getCountyCenter(stateAbbr, countyRaw) {
  const stateMap = countyCenters[stateAbbr];
  if (!stateMap) return null;

  const norm = normalizeCountyName(countyRaw);
  const lower = norm.toLowerCase();

  for (const k of Object.keys(stateMap)) {
    if (k.toLowerCase() === lower) return stateMap[k];
  }
  return null;
}

function tryCounty(text) {
  const re =
    /\b([a-z0-9 .'\-]+?)\s+(county|parish)\b[, ]*\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WV|WI|WY)\b/gi;

  const points = [];
  let m;
  while ((m = re.exec(text))) {
    const county = m[1];
    const st = m[3].toUpperCase();
    const pt = getCountyCenter(st, county);
    if (pt) points.push(pt);
  }
  if (!points.length) return null;

  const lon = points.reduce((s,p)=>s+p[0],0)/points.length;
  const lat = points.reduce((s,p)=>s+p[1],0)/points.length;

  return {
    coordinates: [lon, lat],
    method: "county-centroid",
  };
}

// ----------------------------------------------------
// Region fallback
// ----------------------------------------------------
function tryRegion(text) {
  const lower = text.toLowerCase();
  for (const region of Object.keys(REGIONAL_CENTROIDS)) {
    if (lower.includes(region)) {
      return {
        coordinates: REGIONAL_CENTROIDS[region],
        method: `us-region-${region.replace(/\s+/g,"-")}`,
      };
    }
  }
  return null;
}

// ----------------------------------------------------
// State fallback
// ----------------------------------------------------
function tryState(text) {
  const lower = text.toLowerCase();
  for (const [name, abbr] of Object.entries(STATE_NAME_TO_CODE)) {
    if (lower.includes(name)) {
      const center = STATE_CENTROIDS[abbr];
      if (center) return {
        coordinates: center,
        method: "state-centroid",
      };
    }
  }
  return null;
}

// ----------------------------------------------------
// Location extraction pipeline (OPTION A)
// ----------------------------------------------------
function extractLocation(textRaw, seedStr) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  if (HARD_BLOCK.test(text)) return null;     // obvious junk
  if (FIGURATIVE.test(text)) return null;     // metaphorical events

  // Hazard detection
  const hMatch = text.match(HAZARD_RE);
  if (!hMatch) return null;

  // Soft context validation
  if (!CONTEXT_RE.test(text)) {
    // still accept for OPTION A (looser rules)
    // but deprioritize to region/state fallback
  }

  // 1) County precision
  const county = tryCounty(text);
  if (county) {
    return {
      geometry: {
        type: "Point",
        coordinates: jitter(county.coordinates, seedStr, 0.18),
      },
      method: county.method,
    };
  }

  // 2) City, ST pattern → state centroid
  const cityRe =
    /\b([A-Za-z.\- ']+?),\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WV|WI|WY)\b/gi;

  let m;
  while ((m = cityRe.exec(text))) {
    const st = m[2].toUpperCase();
    const center = STATE_CENTROIDS[st];
    if (center) {
      return {
        geometry: {
          type: "Point",
          coordinates: jitter(center, seedStr, 0.20),
        },
        method: "city-state-centroid",
      };
    }
  }

  // 3) Regional fallback
  const region = tryRegion(text);
  if (region) {
    return {
      geometry: {
        type: "Point",
        coordinates: jitter(region.coordinates, seedStr, 0.28),
      },
      method: region.method,
    };
  }

  // 4) State fallback
  const st = tryState(text);
  if (st) {
    return {
      geometry: {
        type: "Point",
        coordinates: jitter(st.coordinates, seedStr, 0.30),
      },
      method: st.method,
    };
  }

  return null;
}

// ----------------------------------------------------
// Normalize NewsAPI → DB doc
// ----------------------------------------------------
function normalizeArticle(article) {
  try {
    const title = (article.title || "").trim();
    const desc = (article.description || "").trim();
    const url = (article.url || "").trim();
    if (!url || !title) return null;

    const text = `${title}\n${desc}`.toLowerCase();
    const seedStr = `newsapi|${url}`;

    const loc = extractLocation(text, seedStr);
    if (!loc) return null;

    return {
      type: "news",
      provider: "NewsAPI",
      title,
      description: desc,
      url,
      domain: (() => {
        try { return new URL(url).hostname.replace(/^www\./, ""); }
        catch { return ""; }
      })(),
      source: article.source?.name || "Unknown",

      publishedAt: new Date(article.publishedAt || Date.now()),
      createdAt: new Date(),
      expires: new Date(Date.now() + 72 * 3600 * 1000),

      geometry: loc.geometry,
      geometryMethod: loc.method,
    };
  } catch (err) {
    console.warn("⚠️ normalizeArticle failed:", err.message);
    return null;
  }
}

// ----------------------------------------------------
// Save to MongoDB
// ----------------------------------------------------
async function saveNewsArticles(articles) {
  const col = getDB().collection("social_signals");

  let saved = 0;
  for (const a of articles) {
    await col.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    saved++;
  }
  console.log(`💾 Saved ${saved} NewsAPI articles`);
}

// ----------------------------------------------------
// Poller
// ----------------------------------------------------
export async function pollNewsAPI() {
  console.log("📰 NewsAPI poll running…");

  if (!NEWS_API_KEY) {
    console.warn("⚠️ Missing NEWS_API_KEY — skipping.");
    return;
  }

  try {
    const q = HAZARD_WORDS.map(w =>
      w.includes(" ") ? `"${w}"` : w
    ).join(" OR ");

    const from = new Date(Date.now() - 6 * 3600 * 1000).toISOString();

    const url =
      `${NEWS_API_URL}?q=${encodeURIComponent(q)}` +
      `&language=en&from=${encodeURIComponent(from)}` +
      `&sortBy=publishedAt&pageSize=100&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 NewsAPI URL:", url.replace(NEWS_API_KEY, "REDACTED"));

    const { data } = await axios.get(url, { timeout: 20000 });
    const raw = data?.articles || [];

    if (!raw.length) {
      console.log("⚠️ NewsAPI returned zero articles.");
      return;
    }

    const normalized = raw.map(normalizeArticle).filter(Boolean);
    console.log(`✅ Normalized ${normalized.length} / ${raw.length}`);

    if (normalized.length) await saveNewsArticles(normalized);
  } catch (err) {
    console.error("❌ NewsAPI error:", err.message);
  }
}
