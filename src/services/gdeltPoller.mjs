import axios from "axios";
import unzipper from "unzipper";
import readline from "readline";
import { getDB } from "../db.js";

const LASTUPDATE_URL =
  "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt";

const USER_AGENT = process.env.GDELT_USER_AGENT || "disaster-help-backend/1.0";

const TTL_HOURS = Number(process.env.GDELT_TTL_HOURS) || 24;
const TTL_MS = TTL_HOURS * 3600 * 1000;

const MAX_SAVE = Number(process.env.GDELT_MAX_SAVE) || 250;
const BATCH_SIZE = Number(process.env.GDELT_BATCH_SIZE) || 200;

const MAX_RUNTIME_MS = Number(process.env.GDELT_MAX_RUNTIME_MS) || 8 * 60 * 1000;
const FLUSH_EVERY_MS = Number(process.env.GDELT_FLUSH_EVERY_MS) || 4000;
const PROGRESS_EVERY_LINES =
  Number(process.env.GDELT_PROGRESS_EVERY_LINES) || 50000;

const ENRICH_PREVIEW = (process.env.GDELT_ENRICH_PREVIEW || "1") !== "0";
const ENRICH_MAX = Number(process.env.GDELT_ENRICH_MAX) || 120;
const ENRICH_CONCURRENCY = Number(process.env.GDELT_ENRICH_CONCURRENCY) || 6;
const ENRICH_TIMEOUT_MS = Number(process.env.GDELT_ENRICH_TIMEOUT_MS) || 5500;
const ENRICH_MAX_BYTES = Number(process.env.GDELT_ENRICH_MAX_BYTES) || 350_000;

const BLOCKED_DOMAIN_SUBSTRINGS = [
  "tmz.com",
  "people.com",
  "perezhilton",
  "hollywoodreporter.com",
  "eonline.com",
  "usmagazine.com",
  "buzzfeed.com",
];

function rewriteGdeltUrl(url) {
  return url.replace(
    /^https?:\/\/data\.gdeltproject\.org\//i,
    "https://storage.googleapis.com/data.gdeltproject.org/"
  );
}

const IDX = Object.freeze({
  GLOBALEVENTID: 0,
  SQLDATE: 1,

  ACTOR1NAME: 6,
  ACTOR2NAME: 16,

  EVENTCODE: 26,
  EVENTROOTCODE: 28,
  GOLDSTEIN: 30,
  AVGTONE: 34,

  ACTOR1GEO_FULLNAME: 36,
  ACTOR1GEO_COUNTRY: 37,
  ACTOR1GEO_ADM1: 38,
  ACTOR1GEO_ADM2: 39,
  ACTOR1GEO_LAT: 40,
  ACTOR1GEO_LON: 41,

  ACTOR2GEO_FULLNAME: 44,
  ACTOR2GEO_COUNTRY: 45,
  ACTOR2GEO_ADM1: 46,
  ACTOR2GEO_ADM2: 47,
  ACTOR2GEO_LAT: 48,
  ACTOR2GEO_LON: 49,

  ACTIONGEO_FULLNAME: 52,
  ACTIONGEO_COUNTRY: 53,
  ACTIONGEO_ADM1: 54,
  ACTIONGEO_ADM2: 55,
  ACTIONGEO_LAT: 56,
  ACTIONGEO_LON: 57,

  DATEADDED: 59,
  SOURCEURL: 60,
});

const MIN_COLUMNS = 61;

function getDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isBlocked(domain) {
  if (!domain) return false;
  const d = domain.toLowerCase();
  return BLOCKED_DOMAIN_SUBSTRINGS.some((b) => d.includes(b));
}

function validLonLat(lon, lat) {
  return (
    Number.isFinite(lon) &&
    Number.isFinite(lat) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

function pickGeo(cols) {
  const cands = [
    {
      method: "action",
      lat: parseFloat(cols[IDX.ACTIONGEO_LAT]),
      lon: parseFloat(cols[IDX.ACTIONGEO_LON]),
      fullName: cols[IDX.ACTIONGEO_FULLNAME],
      country: cols[IDX.ACTIONGEO_COUNTRY],
      adm1: cols[IDX.ACTIONGEO_ADM1],
      adm2: cols[IDX.ACTIONGEO_ADM2],
      bonus: 0.4,
    },
    {
      method: "actor1",
      lat: parseFloat(cols[IDX.ACTOR1GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR1GEO_LON]),
      fullName: cols[IDX.ACTOR1GEO_FULLNAME],
      country: cols[IDX.ACTOR1GEO_COUNTRY],
      adm1: cols[IDX.ACTOR1GEO_ADM1],
      adm2: cols[IDX.ACTOR1GEO_ADM2],
      bonus: 0.2,
    },
    {
      method: "actor2",
      lat: parseFloat(cols[IDX.ACTOR2GEO_LAT]),
      lon: parseFloat(cols[IDX.ACTOR2GEO_LON]),
      fullName: cols[IDX.ACTOR2GEO_FULLNAME],
      country: cols[IDX.ACTOR2GEO_COUNTRY],
      adm1: cols[IDX.ACTOR2GEO_ADM1],
      adm2: cols[IDX.ACTOR2GEO_ADM2],
      bonus: 0.1,
    },
  ];

  let best = null;
  for (const x of cands) {
    if (!validLonLat(x.lon, x.lat)) continue;

    let score = x.bonus;
    if (x.adm1) score += 1.5;
    if (x.adm2) score += 1.0;
    if (x.fullName) score += Math.min(4, String(x.fullName).split(",").length);

    if (!best || score > best.score) best = { ...x, score };
  }

  return best;
}

function microJitter(lon, lat, seed, mag = 0.12) {
  let h = 0x811c9dc5;
  const s = String(seed || "");
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  const rand = () => {
    h += 0x6d2b79f5;
    let t = Math.imul(h ^ (h >>> 15), 1 | h);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const jLon = lon + (rand() - 0.5) * mag;
  const jLat = lat + (rand() - 0.5) * mag;
  return validLonLat(jLon, jLat) ? [jLon, jLat] : [lon, lat];
}

function nicePlaceName(raw) {
  if (!raw) return "Unknown location";
  const s = String(raw).split(",")[0].trim();
  return s || "Unknown location";
}

function parseDateAdded(dateAdded, fallback) {
  if (!dateAdded || !/^\d{14}$/.test(dateAdded)) return fallback;
  const Y = dateAdded.slice(0, 4);
  const M = dateAdded.slice(4, 6);
  const D = dateAdded.slice(6, 8);
  const h = dateAdded.slice(8, 10);
  const m = dateAdded.slice(10, 12);
  const s = dateAdded.slice(12, 14);
  const d = new Date(`${Y}-${M}-${D}T${h}:${m}:${s}Z`);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

const HAZARD_PATTERNS = [
  { label: "Flood", re: /\b(flood|flooding|flash flood|inundat|storm surge|levee|dam burst)\b/i },
  { label: "Tornado", re: /\b(tornado|twister|waterspout)\b/i },
  { label: "Hurricane / Tropical", re: /\b(hurricane|tropical storm|cyclone|typhoon)\b/i },
  { label: "Severe storm", re: /\b(thunderstorm|severe storm|hail|microburst|downburst)\b/i },
  { label: "High wind", re: /\b(high winds?|strong winds?|damaging winds?|windstorm|gusty)\b/i },
  { label: "Winter storm", re: /\b(blizzard|winter storm|snowstorm|whiteout|ice storm)\b/i },
  { label: "Wildfire", re: /\b(wild ?fire|forest fire|brush fire|grass fire|bushfire)\b/i },
  { label: "Earthquake", re: /\b(earthquake|aftershock|seismic)\b/i },
  { label: "Tsunami", re: /\b(tsunami)\b/i },
  { label: "Volcano", re: /\b(volcano|lava|eruption|ash plume)\b/i },
  { label: "Landslide", re: /\b(landslide|mudslide|debris flow|rockslide)\b/i },
  { label: "Extreme heat", re: /\b(heat wave|heatwave|excessive heat|dangerous heat)\b/i },
  { label: "Drought", re: /\b(drought|water shortage)\b/i },
  { label: "Power outage", re: /\b(power outage|blackout|power cut|grid failure|downed lines?)\b/i },
  { label: "Explosion / Hazmat", re: /\b(explosion|blast|hazmat|chemical spill|toxic leak|gas leak)\b/i },
];

const DAMAGE_PATTERN =
  /\b(evacuate(d|s|ing)?|rescue(d|s|ing)?|fatalit(ies|y)|killed|dead|injured|missing|displaced|destroyed|damaged|washed (away|out)|collapsed|closed roads?|road closures?|state of emergency)\b/i;

const CONTEXT_RE =
  /\b(rain|storm|flood|wind|hail|lightning|snow|ice|earthquake|wildfire|mudslide|landslide|tsunami|volcano|heat|drought|hurricane|typhoon|cyclone|atmospheric river)\b/i;

const HAZARD_EVENTCODES = new Set(["102", "103", "190", "191", "193", "194", "195"]);

function detectHazardFromText(t) {
  if (!t) return null;
  for (const { label, re } of HAZARD_PATTERNS) {
    if (re.test(t)) return label;
  }
  return null;
}

function classifyHazard({ actor1, actor2, place, url, domain, eventCode }) {
  const combined = [actor1, actor2, place, url, domain]
    .join(" ")
    .replace(/[-_/]+/g, " ")
    .toLowerCase();

  const direct = detectHazardFromText(combined);
  if (direct) return direct;

  const hasContext = CONTEXT_RE.test(combined);
  if (hasContext && DAMAGE_PATTERN.test(combined)) return "Significant impact event";

  if (hasContext && HAZARD_EVENTCODES.has(String(eventCode || "").trim())) {
    return "Disaster / emergency event";
  }

  return null;
}

function decodeHtml(s) {
  return String(s || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

function sanitizeText(s, maxLen) {
  const t = decodeHtml(String(s || ""))
    .replace(/\s+/g, " ")
    .trim();
  if (!t) return "";
  return maxLen && t.length > maxLen ? t.slice(0, maxLen - 1).trimEnd() + "…" : t;
}

function extractFromMetaTags(head, wantedKeys) {
  const out = {};
  const tags = head.match(/<meta\b[^>]*>/gi) || [];
  for (const tag of tags) {
    const attrs = {};
    const re = /([:\w-]+)\s*=\s*["']([^"']*)["']/g;
    let m;
    while ((m = re.exec(tag))) {
      attrs[m[1].toLowerCase()] = m[2];
    }
    const prop = (attrs.property || "").toLowerCase();
    const name = (attrs.name || "").toLowerCase();
    const key = prop || name;
    if (!key) continue;
    if (!wantedKeys.has(key)) continue;
    const content = attrs.content || "";
    if (!content) continue;
    if (!out[key]) out[key] = sanitizeText(content, 500);
  }
  return out;
}

function extractTitleTag(head) {
  const m = head.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? sanitizeText(m[1], 180) : "";
}

async function fetchPreview(url) {
  try {
    const resp = await axios.get(url, {
      timeout: ENRICH_TIMEOUT_MS,
      responseType: "text",
      maxRedirects: 5,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "text/html,application/xhtml+xml",
      },
      validateStatus: (s) => s >= 200 && s < 400,
      maxContentLength: ENRICH_MAX_BYTES,
      maxBodyLength: ENRICH_MAX_BYTES,
    });

    const html = String(resp.data || "");
    const head = html.slice(0, ENRICH_MAX_BYTES);

    const wanted = new Set([
      "og:title",
      "twitter:title",
      "og:description",
      "twitter:description",
      "description",
      "og:site_name",
      "application-name",
      "og:image",
      "twitter:image",
    ]);

    const meta = extractFromMetaTags(head, wanted);

    const headline =
      meta["og:title"] || meta["twitter:title"] || extractTitleTag(head);

    const summary =
      meta["og:description"] || meta["twitter:description"] || meta["description"];

    const siteName = meta["og:site_name"] || meta["application-name"] || "";

    const image = meta["og:image"] || meta["twitter:image"] || "";

    return {
      headline: sanitizeText(headline, 160),
      summary: sanitizeText(summary, 260),
      siteName: sanitizeText(siteName, 60),
      image: sanitizeText(image, 400),
    };
  } catch {
    return null;
  }
}

function buildSourceLine(siteName, domain) {
  const base = "News report";
  const s = sanitizeText(siteName, 60);
  if (s) return `${base} • ${s}`;
  if (domain) return `${base} • ${domain}`;
  return base;
}

let GDELT_RUNNING = false;

async function enrichDocs(col, items) {
  const queue = items.slice();
  let totalOps = 0;
  let bulk = [];

  async function flush(reason) {
    if (!bulk.length) return;
    const ops = bulk.length;
    try {
      await col.bulkWrite(bulk, { ordered: false });
      totalOps += ops;
    } catch {
      // ignore enrichment failures (ingest already succeeded)
    } finally {
      bulk = [];
    }
  }

  async function worker() {
    while (true) {
      const item = queue.pop();
      if (!item) return;

      const preview = await fetchPreview(item.url);

      const now = new Date();
      const siteName = preview?.siteName || "";
      const sourceLine = buildSourceLine(siteName, item.domain);

      const updates = {
        source: sourceLine,
        updatedAt: now,
      };

      if (preview?.headline) updates.title = preview.headline;

      if (preview?.summary) {
        updates.description = preview.summary;
      } else {
        updates.description = `${item.hazardLabel} reported near ${nicePlaceName(
          item.place
        )}.`;
      }

      if (siteName) updates.publisherName = siteName;
      if (preview?.image) updates.image = preview.image;

      bulk.push({
        updateOne: {
          filter: { url: item.url },
          update: { $set: updates },
        },
      });

      if (bulk.length >= 200) await flush("batch");
    }
  }

  const workers = Array.from(
    { length: Math.max(1, ENRICH_CONCURRENCY) },
    () => worker()
  );
  await Promise.all(workers);
  await flush("final");

  return totalOps;
}

export async function pollGDELT() {
  if (GDELT_RUNNING) {
    console.log("⏭️ GDELT skipped (previous run still in progress)");
    return;
  }
  GDELT_RUNNING = true;

  const startedAt = Date.now();
  const deadline = startedAt + MAX_RUNTIME_MS;

  let zipStream = null;
  let csvStream = null;
  let rl = null;

  let scanned = 0;
  let saved = 0;
  let flushed = 0;
  let enrichedOps = 0;

  try {
    const { data: lastTxt } = await axios.get(LASTUPDATE_URL, {
      timeout: 15000,
      responseType: "text",
      headers: { "User-Agent": USER_AGENT },
    });

    const line = String(lastTxt)
      .split(/\r?\n/)
      .find((l) => l.includes(".export.CSV.zip"));

    if (!line) {
      console.warn("⚠️ No GDELT export ZIP found");
      return;
    }

    const originalUrl = line.trim().split(/\s+/).pop();
    const zipUrl = rewriteGdeltUrl(originalUrl);

    const zipResp = await axios.get(zipUrl, {
      responseType: "stream",
      timeout: 60000,
      headers: { "User-Agent": USER_AGENT },
    });

    zipStream = zipResp.data;
    csvStream = zipStream.pipe(unzipper.ParseOne(/\.csv$/i));
    rl = readline.createInterface({ input: csvStream, crlfDelay: Infinity });

    const db = getDB();
    const col = db.collection("social_signals");

    const now = new Date();
    await col.deleteMany({ ingest: "GDELT", expires: { $lte: now } });

    let bulk = [];
    let lastFlushAt = Date.now();
    const toEnrich = [];

    async function flushBulk(reason) {
      if (!bulk.length) return;
      const ops = bulk.length;
      try {
        await col.bulkWrite(bulk, { ordered: false });
        flushed += ops;
      } catch (err) {
        console.error("❌ GDELT bulkWrite error:", err?.message || err);
      } finally {
        bulk = [];
        lastFlushAt = Date.now();
      }
    }

    for await (const line of rl) {
      if (Date.now() > deadline) break;
      if (!line) continue;

      scanned++;
      if (scanned % PROGRESS_EVERY_LINES === 0) {
        console.log(
          `…GDELT progress scanned=${scanned} saved=${saved} flushed=${flushed}`
        );
      }

      const cols = line.split("\t");
      if (cols.length < MIN_COLUMNS) continue;

      const url = cols[IDX.SOURCEURL];
      if (!url || !/^https?:\/\//i.test(url)) continue;

      const domain = getDomain(url);
      if (isBlocked(domain)) continue;

      const geo = pickGeo(cols);
      if (!geo) continue;

      const actor1 = cols[IDX.ACTOR1NAME] || "";
      const actor2 = cols[IDX.ACTOR2NAME] || "";
      const place = geo.fullName || "";
      const eventCode = cols[IDX.EVENTCODE] || "";

      const hazardLabel = classifyHazard({
        actor1,
        actor2,
        place,
        url,
        domain,
        eventCode,
      });

      if (!hazardLabel) continue;

      const now = new Date();
      const dateAdded = cols[IDX.DATEADDED];
      const publishedAt = parseDateAdded(dateAdded, now);
      const expires = new Date(Date.now() + TTL_MS);

      const [jLon, jLat] = microJitter(
        geo.lon,
        geo.lat,
        `${cols[IDX.GLOBALEVENTID]}|${url}`,
        0.12
      );

      const baseTitle = `${hazardLabel} near ${nicePlaceName(place)}`;
      const baseDesc = `${hazardLabel} reported near ${nicePlaceName(place)}.`;

      const sourceLine = buildSourceLine("", domain);

      const doc = {
        type: "news",
        source: sourceLine,
        provider: null,
        ingest: "GDELT",

        hazardLabel,
        domain,
        url,

        title: baseTitle,
        description: baseDesc,

        publishedAt,
        updatedAt: now,
        expires,

        geometry: { type: "Point", coordinates: [jLon, jLat] },
        lat: jLat,
        lon: jLon,
        geometryMethod: `gdelt-${geo.method}`,

        place: place || null,

        gdelt: {
          globalEventId: cols[IDX.GLOBALEVENTID],
          sqlDate: cols[IDX.SQLDATE],
          dateAdded,
          eventCode: String(cols[IDX.EVENTCODE] || "").trim(),
          rootCode: String(cols[IDX.EVENTROOTCODE] || "").trim(),
          goldstein: Number(cols[IDX.GOLDSTEIN]),
          avgTone: Number(cols[IDX.AVGTONE]),
          actor1,
          actor2,
          country: geo.country || "",
          geoMethod: geo.method,
        },
      };

      bulk.push({
        updateOne: {
          filter: { url },
          update: { $set: doc, $setOnInsert: { createdAt: now } },
          upsert: true,
        },
      });

      if (ENRICH_PREVIEW && toEnrich.length < ENRICH_MAX) {
        toEnrich.push({ url, domain, hazardLabel, place });
      }

      saved++;

      if (bulk.length >= BATCH_SIZE) {
        await flushBulk("batch");
      } else if (Date.now() - lastFlushAt > FLUSH_EVERY_MS) {
        await flushBulk("timer");
      }

      if (saved >= MAX_SAVE) break;
    }

    await flushBulk("final");

    if (ENRICH_PREVIEW && toEnrich.length) {
      enrichedOps = await enrichDocs(col, toEnrich);
    }

    const runtimeSec = Math.round((Date.now() - startedAt) / 1000);
    console.log(
      `✅ GDELT done scanned=${scanned} saved=${saved} enrichedOps=${enrichedOps} runtimeSec=${runtimeSec}`
    );
  } catch (err) {
    console.error("❌ GDELT ERROR:", err?.message || err);
  } finally {
    GDELT_RUNNING = false;
    try {
      rl?.close();
    } catch {}
    try {
      csvStream?.destroy();
    } catch {}
    try {
      zipStream?.destroy();
    } catch {}
  }
}
