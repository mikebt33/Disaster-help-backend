// news/socialNewsPoller.mjs — Option B (Recommended MVP)
// -------------------------------------------------------
// Major improvements vs your previous version:
// • Regional fallback (Plains, Midwest, Southeast, Gulf Coast, etc.)
// • State-only fallback now triggers placement reliably
// • Far improved hazard context detection
// • Wider NewsAPI yield — without noise
// • Deterministic jitter to avoid stacking
// • Guaranteed clean GeoJSON geometry
// • US-only signals, but much easier to match

import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

console.log("NEWS_API_KEY loaded:", !!NEWS_API_KEY);

/* ---------------------------------------------------------
   Load county centers (for county-level precision)
--------------------------------------------------------- */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

/* ---------------------------------------------------------
   Precompute state centroids
--------------------------------------------------------- */
const STATE_CENTROIDS = {};
for (const [st, counties] of Object.entries(countyCenters)) {
  const coords = Object.values(counties).filter((v) => Array.isArray(v));
  if (coords.length) {
    STATE_CENTROIDS[st] = [
      coords.reduce((s, c) => s + c[0], 0) / coords.length, // lon
      coords.reduce((s, c) => s + c[1], 0) / coords.length, // lat
    ];
  }
}

/* ---------------------------------------------------------
   Regional fallback definitions (Option B upgrade)
--------------------------------------------------------- */
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
  "pacific northwest": [-121.0, 45.5],
  "rockies": [-109.5, 43.0],
  "southwest": [-111.5, 34.0],
};

/* ---------------------------------------------------------
   US state maps
--------------------------------------------------------- */
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
  wisconsin: "WI", wyoming: "WY", "district of columbia": "DC",
};

const STATE_CODES = new Set(Object.values(STATE_NAME_TO_CODE));
const STATE_NAMES = new Set(Object.keys(STATE_NAME_TO_CODE));

/* ---------------------------------------------------------
   Hazard keywords
--------------------------------------------------------- */
const HAZARD_WORDS = [
  "flash flood", "flood", "tornado", "severe weather", "thunderstorm",
  "hurricane", "tropical storm", "winter storm", "blizzard",
  "wildfire", "forest fire",
  "earthquake", "tsunami",
  "landslide", "mudslide",
  "power outage", "heat wave", "extreme heat",
];

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const HAZARD_RE = new RegExp(
  "\\b(" + HAZARD_WORDS.sort((a, b) => b.length - a.length).map(esc).join("|") + ")\\b",
  "gi"
);

/* ---------------------------------------------------------
   Query terms for NewsAPI
--------------------------------------------------------- */
const QUERY_TERMS = [
  "tornado", "flash flood", "flood", "wildfire", "hurricane",
  "tropical storm", "winter storm", "blizzard",
  "earthquake", "power outage", "severe weather",
  "heat wave", "extreme heat", "landslide", "mudslide",
];

function buildOrQuery(terms) {
  return terms
    .map((t) => (t.includes(" ") ? `"${t}"` : t))
    .join(" OR ");
}

/* ---------------------------------------------------------
   Noise blockers
--------------------------------------------------------- */
const HARD_BLOCK =
  /\b(taylor swift|grammy|oscars?|concert|celebrity|museum|painting|album|movie|music video)\b/i;

const FIGURATIVE =
  /\b(fans?\s+flood|sales?\s+flood|media\s+storm|political\s+storm|stormed\s+the)\b/i;

const CONTEXT_VALIDATORS =
  /\b(national weather service|nws|evac|rain|snow|ice|hail|wind|mph|gust|storm surge|landfall|burn|firefighters?)\b/i;

/* ---------------------------------------------------------
   Deterministic jitter (avoid stacking)
--------------------------------------------------------- */
function hash32(str) {
  const s = String(str ?? "");
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return function rand() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function jitter([lon, lat], seed, deg = 0.18) {
  const r = mulberry32(hash32(seed));
  return [lon + (r() - 0.5) * deg, lat + (r() - 0.5) * deg];
}

/* ---------------------------------------------------------
   Extraction helpers
--------------------------------------------------------- */
function tryRegionalFallback(text) {
  text = text.toLowerCase();
  for (const region of Object.keys(REGIONAL_CENTROIDS)) {
    if (text.includes(region)) {
      return {
        point: jitter(REGIONAL_CENTROIDS[region], `region|${region}`, 0.35),
        method: `us-region-${region.replace(/\s+/g, "-")}`,
        confidence: 1,
      };
    }
  }
  return null;
}

function fallbackStateOnly(text) {
  text = text.toLowerCase();
  for (const name of STATE_NAMES) {
    if (text.includes(name)) {
      const abbr = STATE_NAME_TO_CODE[name];
      const center = STATE_CENTROIDS[abbr];
      if (!center) continue;
      return {
        point: jitter(center, `state|${abbr}`, 0.28),
        method: "state-name",
        confidence: 1,
      };
    }
  }
  return null;
}

/* ---------------------------------------------------------
   County match (best) — similar to your old version
--------------------------------------------------------- */
function normalizeCountyName(raw) {
  if (!raw) return "";
  let s = String(raw).trim();
  s = s.replace(/^city of\s+/i, "");
  s = s.replace(
    /\s+(county|parish|borough|census area|municipio|municipality|city)$/i,
    ""
  );
  s = s.replace(/^st[.\s]+/i, "saint ");
  return s.replace(/\./g, "").replace(/\s+/g, " ").trim();
}

function getCountyCenter(stateAbbr, countyRaw) {
  const stateMap = countyCenters?.[stateAbbr];
  if (!stateMap) return null;
  const norm = normalizeCountyName(countyRaw);
  if (!norm) return null;
  if (stateMap[norm]) return stateMap[norm];

  const lower = norm.toLowerCase();
  for (const k of Object.keys(stateMap)) {
    if (String(k).toLowerCase() === lower) return stateMap[k];
  }

  return null;
}

function tryCountyState(text) {
  const matches = [];
  const re =
    /\b([a-z0-9 .'\-]+?)\s+(county|parish)\b[, ]+\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WV|WI|WY)\b/gi;

  let m;
  while ((m = re.exec(text))) {
    const county = m[1];
    const st = m[3].toUpperCase();
    const p = getCountyCenter(st, county);
    if (p) matches.push(p);
  }

  if (!matches.length) return null;

  const lon = matches.reduce((s, p) => s + p[0], 0) / matches.length;
  const lat = matches.reduce((s, p) => s + p[1], 0) / matches.length;

  return {
    point: jitter([lon, lat], `county|${matches.length}`, 0.12),
    method: matches.length > 1 ? "county-centroid" : "county-center",
    confidence: matches.length > 1 ? 3 : 4,
  };
}

/* ---------------------------------------------------------
   Extract location (Option B pipeline)
--------------------------------------------------------- */
function extractLocation(textRaw, seedStr) {
  if (!textRaw) return null;
  const text = textRaw.toLowerCase();

  if (HARD_BLOCK.test(text) || FIGURATIVE.test(text)) return null;

  const hazard = [...text.matchAll(HAZARD_RE)];
  if (!hazard.length) return null;

  // Confirm context
  let confirmed = false;
  for (const h of hazard) {
    const i = h.index || 0;
    const win = text.slice(Math.max(0, i - 500), i + 500);
    if (CONTEXT_VALIDATORS.test(win)) {
      confirmed = true;
      break;
    }
  }
  if (!confirmed) return null;

  // Best → county
  const county = tryCountyState(text);
  if (county) return {
    geometry: { type: "Point", coordinates: county.point },
    geometryMethod: county.method,
    place: { state: county.state ?? null, confidence: county.confidence },
  };

  // City, ST → state centroid (your earlier logic)
  const cityRe =
    /\b([A-Za-z.\- ']+?),\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WV|WI|WY)\b/gi;

  let cm;
  while ((cm = cityRe.exec(text))) {
    const st = cm[2].toUpperCase();
    const stateCenter = STATE_CENTROIDS[st];
    if (stateCenter) {
      return {
        geometry: { type: "Point", coordinates: jitter(stateCenter, seedStr, 0.18) },
        geometryMethod: "city-state→state-centroid",
        place: { state: st, confidence: 2 },
      };
    }
  }

  // Region fallback (new in Option B)
  const region = tryRegionalFallback(text);
  if (region) return {
    geometry: { type: "Point", coordinates: region.point },
    geometryMethod: region.method,
    place: { state: null, confidence: region.confidence },
  };

  // State fallback
  const stOnly = fallbackStateOnly(text);
  if (stOnly) return {
    geometry: { type: "Point", coordinates: stOnly.point },
    geometryMethod: stOnly.method,
    place: { state: null, confidence: stOnly.confidence },
  };

  return null;
}

/* ---------------------------------------------------------
   Normalize article → DB doc
--------------------------------------------------------- */
function normalizeArticle(article) {
  try {
    const title = String(article?.title || "").trim();
    if (!title) return null;

    const desc = String(article?.description || "").trim();
    const fullText = `${title}\n${desc}`.toLowerCase();

    const url = String(article?.url || "").trim();
    if (!url) return null;
    const seedStr = `newsapi|${url}`;

    const publishedAt = new Date(article?.publishedAt || Date.now());

    let domain = "";
    try {
      domain = new URL(url).hostname.replace(/^www\./, "");
    } catch {}

    const loc = extractLocation(fullText, seedStr);
    if (!loc) return null;

    return {
      type: "news",
      provider: "NewsAPI",
      title,
      description: desc,
      url,
      domain,
      source: article?.source?.name || domain || "Unknown",

      publishedAt,
      createdAt: new Date(),
      expires: new Date(Date.now() + 72 * 3600 * 1000),

      geometry: loc.geometry,
      geometryMethod: loc.geometryMethod,
      place: loc.place,
    };
  } catch {
    return null;
  }
}

/* ---------------------------------------------------------
   Save
--------------------------------------------------------- */
async function saveNewsArticles(articles) {
  const db = getDB();
  const col = db.collection("social_signals");

  let saved = 0;
  for (const a of articles) {
    await col.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    saved++;
  }
  console.log(`💾 Saved ${saved} NewsAPI articles`);
}

/* ---------------------------------------------------------
   Poller
--------------------------------------------------------- */
export async function pollNewsAPI() {
  console.log("📰 NewsAPI poll running…");

  if (!NEWS_API_KEY) {
    console.warn("⚠️ Missing NEWS_API_KEY");
    return;
  }

  try {
    const q = buildOrQuery(QUERY_TERMS);
    const from = new Date(Date.now() - 6 * 3600 * 1000).toISOString();

    const url =
      `${NEWS_API_URL}?q=${encodeURIComponent(q)}` +
      `&language=en&from=${encodeURIComponent(from)}` +
      `&sortBy=publishedAt&pageSize=50&apiKey=${NEWS_API_KEY}`;

    console.log("🔍 NewsAPI URL:", url.replace(NEWS_API_KEY, "REDACTED"));

    const { data } = await axios.get(url, { timeout: 20000 });

    if (!data?.articles?.length) {
      console.log("⚠️ NewsAPI returned zero articles.");
      return;
    }

    const normalized = data.articles.map(normalizeArticle).filter(Boolean);

    console.log(`✅ Normalized ${normalized.length} of ${data.articles.length}`);

    if (normalized.length) await saveNewsArticles(normalized);
  } catch (err) {
    console.error("❌ NewsAPI error:", err.response?.data || err.message);
  }
}
