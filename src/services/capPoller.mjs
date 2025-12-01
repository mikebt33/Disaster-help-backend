// src/services/capPoller.mjs
import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const countyCentersPath = path.resolve(__dirname, "../data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

/**
 * CAP Alert Poller Service — NWS / FEMA / USGS
 * Fetches CAP/GeoRSS feeds, normalizes, saves geometry + bbox for map rendering.
 */

const CAP_FEEDS = [
  { name: "NWS National Feed", url: "https://alerts.weather.gov/cap/us.php?x=0", source: "NWS" },
  { name: "FEMA IPAWS", url: "https://ipaws.nws.noaa.gov/feeds/IPAWSOpenCAP.xml", source: "FEMA" },
  { name: "USGS Earthquakes", url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.atom", source: "USGS" },
  // --- State-level NOAA feeds ---
  ...[
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY",
    "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","PR","GU","AS","MP","VI"
  ].map((s) => ({
    name: `${s} NWS Feed`,
    url: `https://alerts.weather.gov/cap/${s.toLowerCase()}.php?x=0`,
    source: "NWS",
    stateHint: s,
  })),
];

/** Fallback centers for state-level alerts (lon, lat) */
const STATE_CENTERS = {
  FL: [-81.5158, 27.6648], TX: [-99.9018, 31.9686], CA: [-119.4179, 36.7783],
  NY: [-75.4999, 43.0003], NC: [-79.0193, 35.7596], VA: [-78.6569, 37.4316],
  GA: [-83.4412, 32.1656], AL: [-86.9023, 32.8067], OH: [-82.9071, 40.4173],
  PA: [-77.1945, 41.2033], MI: [-84.5361, 44.1822], LA: [-91.9623, 30.9843],
  IL: [-89.3985, 40.6331], IN: [-86.1349, 40.2672], SC: [-81.1637, 33.8361],
  KY: [-84.27, 37.8393], TN: [-86.5804, 35.5175], AR: [-92.3731, 34.9697],
  AZ: [-111.0937, 34.0489], CO: [-105.7821, 39.5501], WA: [-120.7401, 47.7511],
  OR: [-120.5542, 43.8041], NV: [-116.4194, 38.8026], OK: [-97.0929, 35.0078],
  MO: [-91.8318, 38.5739], WI: [-89.6165, 44.7863], MN: [-94.6859, 46.7296],
  IA: [-93.0977, 41.878], KS: [-98.4842, 39.0119], ME: [-69.4455, 45.2538],
  VT: [-72.5778, 44.5588], NH: [-71.5724, 43.1939], MA: [-71.3824, 42.4072],
  CT: [-72.6979, 41.6032], RI: [-71.4774, 41.5801], DE: [-75.5277, 38.9108],
  MD: [-76.6413, 39.0458], WV: [-80.4549, 38.5976], ND: [-100.5407, 47.5515],
  SD: [-99.9018, 43.9695], MT: [-110.3626, 46.8797], NE: [-99.9018, 41.4925],
  NM: [-105.8701, 34.5199], WY: [-107.2903, 43.0759], ID: [-114.742, 44.0682],
  UT: [-111.0937, 39.32], AK: [-152.4044, 64.2008], HI: [-155.5828, 19.8968],
  PR: [-66.5901, 18.2208], GU: [144.7937, 13.4443], VI: [-64.8963, 18.3358],
  DC: [-77.0369, 38.9072],
};

const STATE_NAME_TO_ABBR = {
  Alabama:"AL", Alaska:"AK", Arizona:"AZ", Arkansas:"AR", California:"CA",
  Colorado:"CO", Connecticut:"CT", Delaware:"DE", Florida:"FL", Georgia:"GA",
  Hawaii:"HI", Idaho:"ID", Illinois:"IL", Indiana:"IN", Iowa:"IA",
  Kansas:"KS", Kentucky:"KY", Louisiana:"LA", Maine:"ME", Maryland:"MD",
  Massachusetts:"MA", Michigan:"MI", Minnesota:"MN", Mississippi:"MS", Missouri:"MO",
  Montana:"MT", Nebraska:"NE", Nevada:"NV", "New Hampshire":"NH", "New Jersey":"NJ",
  "New Mexico":"NM", "New York":"NY", "North Carolina":"NC", "North Dakota":"ND",
  Ohio:"OH", Oklahoma:"OK", Oregon:"OR", Pennsylvania:"PA", "Rhode Island":"RI",
  "South Carolina":"SC", "South Dakota":"SD", Tennessee:"TN", Texas:"TX", Utah:"UT",
  Vermont:"VT", Virginia:"VA", Washington:"WA", "West Virginia":"WV", Wisconsin:"WI",
  Wyoming:"WY", "District of Columbia":"DC", "Puerto Rico":"PR", Guam:"GU",
  "U.S. Virgin Islands":"VI", "US Virgin Islands":"VI", "American Samoa":"AS",
  "Northern Mariana Islands":"MP"
};

function stateToAbbr(s) {
  if (!s) return null;
  const t = s.trim();
  if (/^[A-Z]{2}$/.test(t)) return t; // already code
  return STATE_NAME_TO_ABBR[t] || null;
}

function extractStateAbbrs(text) {
  if (!text) return [];
  const set = new Set();

  // 2-letter codes
  const codes = text.match(/\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR|GU|AS|MP|VI)\b/g) || [];
  codes.forEach(c => set.add(c));

  // Full names
  for (const name of Object.keys(STATE_NAME_TO_ABBR)) {
    const re = new RegExp(`\\b${name}\\b`, "i");
    if (re.test(text)) set.add(STATE_NAME_TO_ABBR[name]);
  }

  return Array.from(set);
}

/* ------------------- Geometry helpers ------------------- */

const medianAbs = (arr) => {
  const v = arr.map((x) => Math.abs(x)).sort((a,b) => a-b);
  const n = v.length;
  if (!n) return 0;
  return n % 2 ? v[(n-1)/2] : (v[n/2-1] + v[n/2]) / 2;
};

function detectLonLatOrder(pairs) {
  const aOutsideLat = pairs.reduce((c,[a]) => c + (Math.abs(a) > 90 ? 1 : 0), 0);
  const bOutsideLat = pairs.reduce((c,[,b]) => c + (Math.abs(b) > 90 ? 1 : 0), 0);
  if (aOutsideLat !== bOutsideLat) return aOutsideLat > bOutsideLat ? "lonlat" : "latlon";
  const aMed = medianAbs(pairs.map(([a]) => a));
  const bMed = medianAbs(pairs.map(([,b]) => b));
  return (aMed - bMed) > (bMed - aMed) ? "lonlat" : "latlon";
}

function parsePolygon(polygonString) {
  if (!polygonString) return null;
  try {
    let rawPairs = [];
    if (polygonString.includes(",")) {
      rawPairs = polygonString.trim().split(/\s+/).map((p) => {
        const [a,b] = p.split(",").map(Number);
        return [a,b];
      });
    } else {
      const nums = polygonString.trim().split(/\s+/).map(Number).filter((x)=>!isNaN(x));
      for (let i=0;i+1<nums.length;i+=2) rawPairs.push([nums[i],nums[i+1]]);
    }
    rawPairs = rawPairs.filter(([a,b])=>!isNaN(a)&&!isNaN(b));
    if (rawPairs.length<3) return null;
    const order=detectLonLatOrder(rawPairs);
    const coords=rawPairs.map(([a,b])=>order==="latlon"?[b,a]:[a,b]);
    const [firstLon,firstLat]=coords[0];
    const [lastLon,lastLat]=coords[coords.length-1];
    if(firstLon!==lastLon||firstLat!==lastLat) coords.push(coords[0]);
    return{type:"Polygon",coordinates:[coords]};
  }catch(err){
    console.warn("⚠️ Failed to parse polygon:",err.message);
    return null;
  }
}

function polygonCentroid(geometry){
  if(!geometry||geometry.type!=="Polygon")return null;
  const pts=geometry.coordinates[0];
  let x=0,y=0,z=0;
  for(const[lon,lat]of pts){
    const latR=(lat*Math.PI)/180,lonR=(lon*Math.PI)/180;
    x+=Math.cos(latR)*Math.cos(lonR);
    y+=Math.cos(latR)*Math.sin(lonR);
    z+=Math.sin(latR);
  }
  const total=pts.length;
  x/=total;y/=total;z/=total;
  const lon=Math.atan2(y,x);
  const hyp=Math.sqrt(x*x+y*y);
  const lat=Math.atan2(z,hyp);
  return{type:"Point",coordinates:[lon*(180/Math.PI),lat*(180/Math.PI)]};
}

/* ------------------- County helpers ------------------- */

function normalizeCountyName(raw) {
  if (!raw) return "";
  let s = String(raw).trim();
  s = s.replace(/^City of\s+/i, "");
  s = s.replace(/\s+(County|Parish|Borough|Census Area|Municipio|Municipality|City)$/i, "");
  s = s.replace(/^St[.\s]+/i, "Saint ");
  s = s.replace(/\./g, "").replace(/\s+/g, " ").trim();
  return s;
}

function tryCountyCenterFromAreaDesc(areaDesc, stateHint) {
  if (!areaDesc) return null;

  // If the feed tells us the state (e.g., NY feed), trust that first.
  // Otherwise fall back to extracting from text (for national/FEMA feeds).
  let stateHints = [];
  if (stateHint) {
    stateHints = [stateHint];
  } else {
    stateHints = extractStateAbbrs(areaDesc); // may be [], [abbr], or multiple
  }

  const regions = areaDesc
    .split(/[,;]+/)
    .map((s) => s.trim())
    .filter(Boolean);

  for (const r of regions) {
    // "Smith County, TX" or "Smith, TX" or "Smith, Texas"
    let m = r.match(/^(.+?),\s*([A-Za-z .]+)$/);
    if (m) {
      const county = normalizeCountyName(m[1]);
      let abbr = stateToAbbr(m[2]); // explicit code/name if present in this region

      // If this region doesn't specify a state, but the feed has exactly one hint,
      // use that (e.g., NY feed).
      if (!abbr && stateHints.length === 1) {
        abbr = stateHints[0];
      }

      if (abbr && countyCenters[abbr]?.[county]) {
        return { type: "Point", coordinates: countyCenters[abbr][county] };
      }
    }

    // County-only token AND exactly one state hint total
    if (stateHints.length === 1 && /^[A-Za-z .'-]+$/.test(r)) {
      const countyOnly = normalizeCountyName(r);
      const abbr = stateHints[0];
      if (countyCenters[abbr]?.[countyOnly]) {
        return { type: "Point", coordinates: countyCenters[abbr][countyOnly] };
      }
    }
  }

  return null;
}

/* ------------------- Normalization ------------------- */

function normalizeCapAlert(entry, feed) {
  try {
    const source = feed.source;
    const stateHint = feed.stateHint || null;

    const root = entry["cap:alert"] || entry.alert || entry.content?.["cap:alert"] || entry;

    const info=Array.isArray(root?.info)?root.info[0]:root?.info||{};
    const area=Array.isArray(info?.area)?info.area[0]:info?.area||{};

    // --- Polygon extraction ---
    let polygonRaw=area?.polygon||area?.["cap:polygon"]||root?.polygon||info?.polygon||null;
    if(Array.isArray(polygonRaw)) polygonRaw=polygonRaw.join(" ");
    const polygonGeom=parsePolygon(polygonRaw);

    // --- Geometry ---
    let geometry=null;
    let geometryMethod=null;

    if(polygonGeom){
      geometry=polygonCentroid(polygonGeom);
      geometryMethod="polygon";
    }

    if(!geometry){
      const pointStr=root?.point||root?.["georss:point"];
      if(typeof pointStr==="string"){
        const parts=pointStr.trim().split(/\s+/).map(Number);
        if(parts.length>=2&&!isNaN(parts[0])&&!isNaN(parts[1])){
          const[lat,lon]=parts;
          geometry={type:"Point",coordinates:[lon,lat]};
          geometryMethod="georss-point";
        }
      }
    }

    if(!geometry){
      const lat=parseFloat(info?.lat||area?.lat);
      const lon=parseFloat(info?.lon||area?.lon);
      if(!isNaN(lat)&&!isNaN(lon)){
        geometry={type:"Point",coordinates:[lon,lat]};
        geometryMethod="explicit-latlon";
      }
    }

    // --- County center lookup to avoid US centroid stacking ---
    if (!geometry && area?.areaDesc) {
      const countyPoint = tryCountyCenterFromAreaDesc(area.areaDesc, stateHint);
      if (countyPoint) {
        geometry = countyPoint;
        geometryMethod = "county-center";
        console.log(
          `✅ County-center match: ${area.areaDesc} [stateHint=${stateHint || "none"}]`
        );
      }
    }

    // --- Fallback to state center (supports codes + full names) ---
    if (!geometry) {
      const desc = area?.areaDesc || root?.areaDesc || "";

      let stateList = [];

      // Prefer the feed's stateHint (e.g., NY feed) if we have one.
      if (stateHint) {
        stateList = [stateHint];
      } else {
        stateList = extractStateAbbrs(desc); // for national/FEMA feeds
      }

      if (stateList.length > 0) {
        const abbr = stateList[0]; // first mention or hint
        if (STATE_CENTERS[abbr]) {
          geometry = { type: "Point", coordinates: STATE_CENTERS[abbr] };
          geometryMethod = "state-center";
          console.log(`🗺️ Fallback to state center: ${abbr} (${area.areaDesc || "(no areaDesc)"})`);
        }
      }
    }

    // --- Skip if still no valid geometry ---
    if (!geometry) {
      console.warn(`🚫 Skipping alert with no geo/county/state: ${area.areaDesc || "(no areaDesc)"}`);
      return null;
    }

    // --- Bounding box ---
    let bbox=null;
    if(polygonGeom?.coordinates?.[0]?.length>2){
      const pts=polygonGeom.coordinates[0];
      const lons=pts.map(p=>p[0]);
      const lats=pts.map(p=>p[1]);
      bbox=[Math.min(...lons),Math.min(...lats),Math.max(...lons),Math.max(...lats)];
    }

    // --- Event labeling + expiration logic (USGS short TTLs) ---
    // --- Normalize expiration to proper Date object ---
    let expiresRaw = info?.expires || root?.expires || null;
    let expires = null;

    if (expiresRaw) {
      const parsed = new Date(expiresRaw);
      if (!isNaN(parsed.getTime())) {
        expires = parsed;
      } else {
        console.warn(`⚠️ Invalid expires format, fallback 1h TTL for ${root?.identifier || "(unknown)"}`);
        expires = new Date(Date.now() + 60 * 60 * 1000);
      }
    } else {
      // Fallback if feed didn’t include expires
      expires = new Date(Date.now() + 60 * 60 * 1000);
    }

    let eventName =
      info?.event ||
      root?.event ||
      (root.title && root.title.split(" issued")[0]) ||
      (root.summary && root.summary.split(" issued")[0]) ||
      "Alert";

    if (source === "USGS") {
      const magMatch = root?.title?.match(/M\s?(\d+\.\d+)/);
      const magnitude = magMatch ? parseFloat(magMatch[1]) : null;
      const sentTime = new Date(info?.effective || root.sent || root.updated || Date.now());

      if (magnitude !== null) {
        if (magnitude < 3.0) {
          eventName = `Seismic Activity (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 10 * 60 * 1000); // 10 minutes
        } else if (magnitude < 5.0) {
          eventName = `Minor Earthquake (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 60 * 60 * 1000); // 1 hour
        } else {
          eventName = `Earthquake (M ${magnitude})`;
          expires = new Date(sentTime.getTime() + 3 * 60 * 60 * 1000); // 3 hours
        }
      } else {
        eventName = "Seismic Activity";
        expires = new Date(sentTime.getTime() + 10 * 60 * 1000); // 10 minutes
      }
    }

    const headlineText =
      info?.headline ||
      root?.title ||
      root?.summary ||
      eventName;

    // --- Description cleanup ---
    let descriptionText = info?.description || root?.summary || root?.content || "";
    if (typeof descriptionText === "object" && descriptionText["#text"]) {
      descriptionText = descriptionText["#text"];
    }
    if (typeof descriptionText !== "string") {
      descriptionText = String(descriptionText ?? "");
    }
    descriptionText = descriptionText
      .replace(/<dt>/g, "\n")
      .replace(/<\/dt>/g, ": ")
      .replace(/<dd>/g, "")
      .replace(/<\/dd>/g, "")
      .replace(/<\/?dl>/g, "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]*>/g, "")
      .replace(/&deg;/g, "°")
      .replace(/[ \t]+/g, " ")
      .replace(/\n\s*\n/g, "\n")
      .trim();

    if (source === "USGS" && descriptionText.match(/UTC/)) {
      descriptionText = descriptionText.replace(/Time/g, "🕒 Time");
      descriptionText = descriptionText.replace(/Location/g, "📍 Location");
      descriptionText = descriptionText.replace(/Depth/g, "🌎 Depth");
    }

    let instructionText = info?.instruction || root?.instruction || "";
    if (typeof instructionText !== "string") {
      instructionText = String(instructionText ?? "");
    }

    const infoBlock = {
      category: info?.category || "General",
      event: eventName.trim(),
      urgency: info?.urgency || (source === "USGS" ? "Past" : "Unknown"),
      severity: info?.severity || (source === "USGS" ? "Minor" : "Unknown"),
      certainty: info?.certainty || (source === "USGS" ? "Observed" : "Unknown"),
      headline: headlineText.trim(),
      description: descriptionText.trim(),
      instruction: instructionText.trim(),
    };

    return{
      identifier:root.identifier||root.id||`UNKNOWN-${Date.now()}`,
      sender:root.sender||"",
      sent:info?.effective||root.sent||root.updated||root.published||new Date().toISOString(),
      status:root.status||"Actual",
      msgType:root.msgType||"Alert",
      scope:root.scope||"Public",
      info:infoBlock,
      area:{areaDesc:area?.areaDesc||info?.areaDesc||root.areaDesc||"",polygon:polygonRaw||null},
      geometry,
      geometryMethod,
      bbox,
      hasGeometry:true, // we skip when false
      title:headlineText.trim(),
      summary:descriptionText.trim(),
      source,
      timestamp:new Date(),
      expires,
    };
  }catch(err){
    console.error("❌ Error normalizing CAP alert:",err.message);
    return null;
  }
}

/** Save parsed alerts into MongoDB */
async function saveAlerts(alerts){
  try{
    const db=getDB();
    const collection=db.collection("alerts_cap");
    const cutoff=new Date(Date.now()-72*60*60*1000);
    await collection.deleteMany({sent:{$lt:cutoff}});
    for(const alert of alerts){
      const doc={...alert};
      if(alert.geometry)doc.geometry=alert.geometry;
      if(alert.bbox)doc.bbox=alert.bbox;
      await collection.updateOne({identifier:alert.identifier},{$set:doc},{upsert:true});
    }
    console.log(`💾 Saved ${alerts.length} alerts to MongoDB`);
  }catch(err){
    console.error("❌ Error saving alerts:",err.message);
  }
}

/** Fetch and process a feed */
async function fetchCapFeed(feed){
  console.log(`🌐 Fetching ${feed.name} (${feed.source})`);
  try{
    const res=await axios.get(feed.url,{timeout:20000});
    const xml=res.data;
    const parser=new XMLParser({ignoreAttributes:false,removeNSPrefix:true});
    const json=parser.parse(xml);
    let entries=json.alert?[json.alert]:json.feed?.entry||[];
    if(!Array.isArray(entries))entries=[entries];
    const alerts = entries.map((e) => normalizeCapAlert(e, feed)).filter(Boolean);
    const usable=alerts.filter(a=>a.hasGeometry).length;
    console.log(`✅ Parsed ${alerts.length} alerts from ${feed.source} (${usable} usable geo)`);
    if(alerts.length)await saveAlerts(alerts);
  }catch(err){
    console.error(`❌ Error fetching ${feed.name}:`,err.message);
  }
}

/** Run all feeds */
async function pollCapFeeds() {
  console.log("🚨 CAP Poller running...");

  // --- TTL cleanup fallback ---
  try {
    const db = getDB();
    const collection = db.collection("alerts_cap");
    const now = new Date();

    // Delete expired or malformed alerts
    const { deletedCount } = await collection.deleteMany({
      $or: [
        { expires: { $lte: now } },
        { expires: { $exists: false } },
        { expires: null }
      ]
    });

    if (deletedCount > 0) {
      console.log(`🧹 Cleaned up ${deletedCount} expired CAP alerts`);
    }
  } catch (err) {
    console.error("⚠️ TTL cleanup failed:", err.message);
  }

  // --- Continue normal polling ---
  for (const feed of CAP_FEEDS) await fetchCapFeed(feed);
  console.log("✅ CAP poll cycle complete.\n");
}

export { pollCapFeeds, tryCountyCenterFromAreaDesc };
