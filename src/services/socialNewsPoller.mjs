// news/socialNewsPoller.mjs
// NewsAPI → social_signals (MVP-reliable)
// Goals:
// - Never “0 results” due to future timestamps / over-strict boolean queries
// - Prefer recent articles (last 6h), no `to=` param
// - Query is specific (avoids generic “storm” noise), then we filter locally
// - US-only via geolocation extraction (county/state); if we can’t locate → drop
// - Deterministic jitter (stable marker position per URL) to reduce stacking
// - Stores a clean GeoJSON Point in `geometry` + `geometryMethod`

import axios from "axios";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

const NEWS_API_URL = "https://newsapi.org/v2/everything";
const NEWS_API_KEY = process.env.NEWS_API_KEY;

console.log("NEWS_API_KEY loaded:", !!NEWS_API_KEY);

/* -------------------- Load county centers -------------------- */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

/* -------------------- Precompute state centroids -------------------- */
const STATE_CENTROIDS = {};
for (const [st, counties] of Object.entries(countyCenters)) {
  const coords = Object.values(counties).filter((v) => Array.isArray(v) && v.length >= 2);
  if (coords.length) {
    STATE_CENTROIDS[st] = [
      coords.reduce((s, c) => s + c[0], 0) / coords.length, // lon
      coords.reduce((s, c) => s + c[1], 0) / coords.length, // lat
    ];
  }
}

/* -------------------- US state maps -------------------- */
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

/* -------------------- Hazard keywords (detection) -------------------- */
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

/* -------------------- Query terms (NewsAPI search) -------------------- */
/**
 * Keep the query tighter than detection. Avoid plain "storm" (too noisy).
 * We still detect "storm" context locally via validators.
 */
const QUERY_TERMS = [
  "tornado",
  "flash flood",
  "flood",
  "wildfire",
  "hurricane",
  "tropical storm",
  "winter storm",
  "blizzard",
  "earthquake",
  "power outage",
  "severe weather",
  "heat wave",
  "extreme heat",
  "landslide",
  "mudslide",
];

function buildOrQuery(terms) {
  return terms
    .map((t) => (t.includes(" ") ? `"${t}"` : t))
    .join(" OR ");
}

/* -------------------- Noise blockers -------------------- */
/**
 * IMPORTANT: do NOT block politics (“Biden”, “White House”, etc) — real disaster coverage mentions it.
 * Keep only high-confidence non-disaster categories.
 */
const HARD_BLOCK =
  /\b(taylor swift|grammy|oscars?|netflix|museum|painting|concert|celebrity|theater|movie|album|music video)\b|\b(stock|market|loan|bailout|stimulus|bond|treasury|fiscal|budget|crypto|bitcoin)\b|\b(shooting|murder|assault|homicide|kidnapping|smuggler|cartel)\b/i;

const FIGURATIVE =
  /\b(fans?\s+flood|sales?\s+flood|tweets?\s+flood|media\s+storm|political\s+storm|firestorm\s+of\s+criticism|stormed\s+the)\b/i;

const CONTEXT_VALIDATORS =
  /\b(nws|national weather service|weather service|warning|watch|advisory|evacu(at|ation)|inundation|rain|snow|ice|hail|wind|mph|gust|river|creek|storm surge|landfall|firefighters?|burn|smoke|containment|aftershock|magnitude|seismic|outage|power lines?)\b/i;

/* -------------------- Tiny geo helpers -------------------- */
function isFiniteLonLat(lon, lat) {
  return Number.isFinite(lon) && Number.isFinite(lat) && Math.abs(lon) <= 180 && Math.abs(lat) <= 90;
}

function wrapLon(lon) {
  let x = lon;
  while (x > 180) x -= 360;
  while (x < -180) x += 360;
  return x;
}

function sanitizePoint([lon, lat]) {
  if (!isFiniteLonLat(lon, lat)) return null;
  return [wrapLon(lon), lat];
}

function centroid(points) {
  const pts = (points || []).map(sanitizePoint).filter(Boolean);
  if (!pts.length) return null;
  const lon = pts.reduce((s, p) => s + p[0], 0) / pts.length;
  const lat = pts.reduce((s, p) => s + p[1], 0) / pts.length;
  return sanitizePoint([lon, lat]);
}

/* -------------------- Deterministic jitter (stable per URL) -------------------- */
// FNV-1a 32-bit
function hash32(str) {
  const s = String(str ?? "");
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// Mulberry32
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

function jitterDeterministic([lon, lat], seedStr, deg = 0.18) {
  const clean = sanitizePoint([lon, lat]);
  if (!clean) return null;
  const r = mulberry32(hash32(seedStr));
  const jLon = clean[0] + (r() - 0.5) * deg;
  const jLat = clean[1] + (r() - 0.5) * deg;
  return sanitizePoint([jLon, jLat]) || clean;
}

/* -------------------- County + State extraction -------------------- */
function normalizeCountyName(raw) {
  if (!raw) return "";
  let s = String(raw).trim();
  s = s.replace(/^city of\s+/i, "");
  s = s.replace(
    /\s+(county|parish|borough|census area|municipio|municipality|city)$/i,
    ""
  );
  s = s.replace(/^st[.\s]+/i, "saint ");
  s = s.replace(/\./g, "").replace(/\s+/g, " ").trim();
  // Title-ish: countyCenters keys may be titlecased; we try multiple forms later.
  return s;
}

// Best-effort lookup: try direct key, then a few normalizations
function getCountyCenter(stateAbbr, countyRaw) {
  const stateMap = countyCenters?.[stateAbbr];
  if (!stateMap) return null;

  const norm = normalizeCountyName(countyRaw);
  if (!norm) return null;

  // Try exact, lower, titlecase-ish
  if (stateMap[norm]) return stateMap[norm];
  const lower = norm.toLowerCase();
  for (const k of Object.keys(stateMap)) {
    if (String(k).toLowerCase() === lower) return stateMap[k];
  }

  return null;
}

function tryCountyState(text) {
  // e.g. "Harris County, TX" / "Cook County IL" / "Orleans Parish, LA"
  const re =
    /\b([a-z0-9 .'\-]+?)\s+(county|parish|borough|census area|municipio|municipality)\b[, ]+\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b/gi;

  const matches = [];
  let m;
  while ((m = re.exec(text))) {
    const county = m[1];
    const st = m[3].toUpperCase();
    const p = getCountyCenter(st, county);
    if (p && Array.isArray(p) && p.length >= 2) matches.push(p);
    if (matches.length >= 8) break;
  }

  const c = centroid(matches);
  if (!c) return null;

  return {
    point: c,
    state: null,
    confidence: matches.length > 1 ? 3 : 4,
    method: matches.length > 1 ? "county-centroid" : "county-center",
  };
}

function tryCityState(text) {
  // "Miami, FL" style. We don't have city centers, so place on state centroid (jittered).
  const re =
    /\b([A-Za-z.\- ']+?),\s*(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/gi;

  let m;
  while ((m = re.exec(text))) {
    const st = m[2].toUpperCase();
    const center = STATE_CENTROIDS[st];
    if (!center) continue;

    return {
      point: center,
      state: st,
      confidence: 2,
      method: "city-state→state-centroid",
    };
  }

  return null;
}

function fallbackState(text) {
  // State code
  const codeRe =
    /\b(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b/i;
  const codeMatch = text.match(codeRe);
  if (codeMatch) {
    const st = codeMatch[1].toUpperCase();
    const center = STATE_CENTROIDS[st];
    if (center) {
      return { point: center, state: st, confidence: 1, method: "state-code" };
    }
  }

  // State name
  for (const name of STATE_NAMES) {
    if (text.includes(name)) {
      const st = STATE_NAME_TO_CODE[name];
      const center = STATE_CENTROIDS[st];
      if (center) {
        return { point: center, state: st, confidence: 1, method: "state-name" };
      }
    }
  }

  return null;
}

/* -------------------- Extract Location (US-only via extraction) -------------------- */
function extractLocation(textRaw, seedStr) {
  if (!textRaw) return null;

  const text = String(textRaw).toLowerCase();

  // Must contain a hazard term
  const hazardMatches = [...text.matchAll(HAZARD_RE)];
  if (hazardMatches.length === 0) return null;

  // Block obvious junk
  if (HARD_BLOCK.test(text) || FIGURATIVE.test(text)) return null;

  // Confirm hazard context near the matches
  let confirmed = false;
  for (const h of hazardMatches) {
    const idx = typeof h.index === "number" ? h.index : 0;
    const start = Math.max(0, idx - 600);
    const end = Math.min(text.length, idx + String(h[0]).length + 600);
    if (CONTEXT_VALIDATORS.test(text.slice(start, end))) {
      confirmed = true;
      break;
    }
  }
  if (!confirmed) return null;

  // 1) County pattern (best US signal)
  const countyHit = tryCountyState(text);
  if (countyHit?.point) {
    const j = jitterDeterministic(countyHit.point, seedStr, 0.12);
    if (!j) return null;
    return {
      geometry: { type: "Point", coordinates: j },
      geometryMethod: countyHit.method,
      place: { state: countyHit.state || null, confidence: countyHit.confidence },
    };
  }

  // 2) City, ST -> state centroid
  const cityHit = tryCityState(text);
  if (cityHit?.point) {
    const j = jitterDeterministic(cityHit.point, seedStr, 0.18);
    if (!j) return null;
    return {
      geometry: { type: "Point", coordinates: j },
      geometryMethod: cityHit.method,
      place: { state: cityHit.state || null, confidence: cityHit.confidence },
    };
  }

  // 3) State fallback
  const stHit = fallbackState(text);
  if (stHit?.point) {
    const j = jitterDeterministic(stHit.point, seedStr, 0.22);
    if (!j) return null;
    return {
      geometry: { type: "Point", coordinates: j },
      geometryMethod: stHit.method,
      place: { state: stHit.state || null, confidence: stHit.confidence },
    };
  }

  return null;
}

/* -------------------- Normalize article -------------------- */
function normalizeArticle(article) {
  try {
    const title = String(article?.title || "").trim();
    if (!title) return null;

    const desc = String(article?.description || "").trim();
    const content = String(article?.content || "").trim();
    const url = String(article?.url || "").trim();
    if (!url) return null;

    let domain = "";
    try {
      domain = new URL(url).hostname.replace(/^www\./, "");
    } catch {}

    const publisher = String(article?.source?.name || domain || "Unknown").trim();
    const publishedAt = new Date(article?.publishedAt || Date.now());

    const fullText = `${title}\n${desc}\n${content}`.toLowerCase();
    const seedStr = `newsapi|${url}`;

    // US-only by extraction: if we can't locate → drop
    let loc = extractLocation(fullText, seedStr);
    if (!loc) loc = extractLocation(title.toLowerCase(), seedStr);
    if (!loc) return null;

    // Safety: ensure geometry is valid
    const coords = loc.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) return null;
    if (!isFiniteLonLat(coords[0], coords[1])) return null;

    return {
      type: "news",
      provider: "NewsAPI",

      title,
      description: desc,
      url,
      domain,
      source: publisher,

      publishedAt,
      createdAt: new Date(),
      // matches your server TTL goal (72h)
      expires: new Date(Date.now() + 72 * 60 * 60 * 1000),

      geometry: loc.geometry,
      geometryMethod: loc.geometryMethod,
      place: loc.place,

      // optional minimal raw fields if you ever want to debug
      newsapi: {
        author: article?.author || null,
      },
    };
  } catch (err) {
    console.warn("❌ Error normalizing:", err.message);
    return null;
  }
}

/* -------------------- Save (upsert by url) -------------------- */
async function saveNewsArticles(articles) {
  const db = getDB();
  const col = db.collection("social_signals");

  let saved = 0;
  for (const a of articles) {
    if (!a?.url) continue;
    await col.updateOne({ url: a.url }, { $set: a }, { upsert: true });
    saved++;
  }

  console.log(`💾 Saved ${saved} NewsAPI articles`);
}

/* -------------------- Poller -------------------- */
export async function pollNewsAPI() {
  console.log("📰 NewsAPI poll running…");

  if (!NEWS_API_KEY) {
    console.warn("⚠️ NEWS_API_KEY missing — skipping NewsAPI poll.");
    return;
  }

  try {
    // MVP: query recent only, no `to=` param, avoid future timestamps
    const q = buildOrQuery(QUERY_TERMS);
    const from = new Date(Date.now() - 6 * 3600 * 1000).toISOString(); // last 6 hours

    const url =
      `${NEWS_API_URL}?` +
      `q=${encodeURIComponent(q)}` +
      `&language=en` +
      `&from=${encodeURIComponent(from)}` +
      `&sortBy=publishedAt` +
      `&pageSize=50` +
      `&apiKey=${encodeURIComponent(NEWS_API_KEY)}`;

    console.log("🔍 URL:", url.replace(NEWS_API_KEY, "REDACTED"));

    const { data } = await axios.get(url, { timeout: 20000 });

    if (!data?.articles?.length) {
      console.log("⚠️ No NewsAPI articles returned (raw=0).");
      return;
    }

    // Normalize + filter
    const normalized = data.articles.map(normalizeArticle).filter(Boolean);

    console.log(`✅ Normalized ${normalized.length} of ${data.articles.length} raw articles`);

    if (normalized.length) await saveNewsArticles(normalized);
  } catch (err) {
    const status = err?.response?.status;
    const msg = err?.response?.data?.message || err.message;

    console.error(`❌ NewsAPI error${status ? ` (${status})` : ""}:`, msg);

    // Helpful hints for common failure modes
    if (status === 401) console.error("↳ Check NEWS_API_KEY (invalid or not enabled for this endpoint).");
    if (status === 429) console.error("↳ Rate limited — reduce poll frequency or pageSize.");
  }
}
