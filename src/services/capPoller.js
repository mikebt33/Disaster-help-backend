import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";

/**
 * CAP Alert Poller Service
 * Full nationwide + territories coverage
 * Normalizes single-object feeds and extracts polygon, circle, and lat/lon.
 */

const CAP_FEEDS = [
  { name: "NWS National Feed", url: "https://alerts.weather.gov/cap/us.php?x=0", source: "NWS" },
  { name: "FEMA IPAWS", url: "https://ipaws.nws.noaa.gov/feeds/IPAWSOpenCAP.xml", source: "FEMA" },
  { name: "USGS Earthquakes", url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.atom", source: "USGS" },
  // 50 states + DC + territories
  "al","ak","az","ar","ca","co","ct","de","dc","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md","ma","mi","mn",
  "ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa",
  "wv","wi","wy","pr","gu","as","mp","vi"
].map((c) => typeof c === "string"
  ? { name: `${c.toUpperCase()} NWS Feed`, url: `https://alerts.weather.gov/cap/${c}.php?x=0`, source: "NWS" }
  : c);

/** Parse polygon strings into GeoJSON Polygon */
function parsePolygon(polygonString) {
  if (!polygonString) return null;
  try {
    const coords = polygonString
      .trim()
      .split(/\s+/)
      .map((p) => {
        const [lat, lon] = p.split(",");
        return [parseFloat(lon), parseFloat(lat)];
      })
      .filter((c) => c.length === 2 && !isNaN(c[0]) && !isNaN(c[1]));
    if (coords.length < 3) return null;
    if (coords[0][0] !== coords[coords.length - 1][0]) coords.push(coords[0]);
    return { type: "Polygon", coordinates: [coords] };
  } catch {
    return null;
  }
}

/** Compute centroid from Polygon */
function polygonCentroid(geometry) {
  if (!geometry || geometry.type !== "Polygon") return null;
  const pts = geometry.coordinates[0];
  let x = 0, y = 0, z = 0;
  for (const [lon, lat] of pts) {
    const latR = (lat * Math.PI) / 180, lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
  }
  const total = pts.length;
  x /= total; y /= total; z /= total;
  const lon = Math.atan2(y, x);
  const hyp = Math.sqrt(x * x + y * y);
  const lat = Math.atan2(z, hyp);
  return { type: "Point", coordinates: [lon * 180 / Math.PI, lat * 180 / Math.PI] };
}

/** Normalize one CAP alert */
function normalizeCapAlert(entry, source) {
  try {
    const info = Array.isArray(entry.info) ? entry.info[0] : entry.info;
    const area = Array.isArray(info?.area) ? info.area[0] : info?.area || {};

    // Normalize polygon (string, array, namespaced)
    let polygonRaw =
      area?.polygon ||
      area?.["cap:polygon"] ||
      info?.polygon ||
      info?.["cap:polygon"];
    if (Array.isArray(polygonRaw)) polygonRaw = polygonRaw.join(" ");

    // Handle multiple polygons in a string
    const polygonGeom = parsePolygon(polygonRaw);
    let geometry = polygonGeom ? polygonCentroid(polygonGeom) : null;

    // Derive bounding box if polygon available
    let bbox = null;
    if (polygonGeom && polygonGeom.coordinates?.[0]?.length >= 3) {
      const pts = polygonGeom.coordinates[0];
      const lats = pts.map((p) => p[1]);
      const lons = pts.map((p) => p[0]);
      bbox = [
        Math.min(...lons),
        Math.min(...lats),
        Math.max(...lons),
        Math.max(...lats),
      ];
    }

    // Parse <circle> like "37.25,-80.10 50"
    const circleField =
      area?.circle ||
      area?.["cap:circle"] ||
      info?.circle ||
      info?.["cap:circle"];
    if (!geometry && circleField) {
      const parts = circleField.split(/[ ,]/).map(parseFloat);
      if (parts.length >= 2 && !isNaN(parts[0]) && !isNaN(parts[1])) {
        geometry = { type: "Point", coordinates: [parts[1], parts[0]] };
      }
    }

    // Fallback lat/lon
    const lat = parseFloat(info?.lat || area?.lat || info?.latitude);
    const lon = parseFloat(info?.lon || area?.lon || info?.longitude);
    if (!geometry && !isNaN(lat) && !isNaN(lon)) {
      geometry = { type: "Point", coordinates: [lon, lat] };
    }

    return {
      identifier: entry.identifier || entry.id || `UNKNOWN-${Date.now()}`,
      sender: entry.sender || "",
      sent: info?.effective || entry.sent || new Date().toISOString(),
      status: entry.status || "Actual",
      msgType: entry.msgType || "Alert",
      scope: entry.scope || "Public",
      info: {
        category: info?.category || "General",
        event: info?.event || "Alert",
        urgency: info?.urgency || "Unknown",
        severity: info?.severity || "Unknown",
        certainty: info?.certainty || "Unknown",
        headline: info?.headline || "",
        description: info?.description || "",
        instruction: info?.instruction || "",
      },
      area: {
        areaDesc: area?.areaDesc || info?.areaDesc || "",
        polygon: polygonRaw || null,
        circle: circleField || null,
      },
      geometry,
      bbox, // ✅ new bounding box field
      source,
      timestamp: new Date(),
    };
  } catch (err) {
    console.error("⚠️ Error normalizing CAP alert:", err.message);
    return null;
  }
}

/** Save alerts into MongoDB */
async function saveAlerts(alerts) {
  const db = getDB();
  const alertsCol = db.collection("alerts_cap");
  const hazardsCol = db.collection("hazards");

  for (const alert of alerts) {
    if (!alert.identifier) continue;
    await alertsCol.updateOne(
      { identifier: alert.identifier },
      { $set: alert },
      { upsert: true }
    );
    if (alert.geometry) {
      await hazardsCol.updateOne(
        { "geometry.coordinates": alert.geometry.coordinates },
        {
          $set: {
            type: alert.info.event || "Alert",
            description: alert.info.description || "",
            severity: alert.info.severity,
            source: alert.source || "CAP",
            geometry: alert.geometry,
            timestamp: new Date(),
          },
        },
        { upsert: true }
      );
    }
  }
}

/** Fetch and process a single feed */
async function fetchCapFeed(feed) {
  console.log(`🌐 Fetching ${feed.name} (${feed.source})`);
  try {
    const res = await axios.get(feed.url, { timeout: 20000 });
    const xml = res.data;
    const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
    const json = parser.parse(xml);

    let entries = [];
    if (json.alert) entries = [json.alert];
    else if (Array.isArray(json.feed?.entry)) entries = json.feed.entry;
    else if (json.feed?.entry) entries = [json.feed.entry];
    else if (Array.isArray(json.alerts)) entries = json.alerts;
    else entries = [];

    const alerts = entries.map((e) => normalizeCapAlert(e, feed.source)).filter(Boolean);
    const withGeo = alerts.filter((a) => a.geometry);

    if (alerts.length) {
      console.log(`✅ Parsed ${alerts.length} alerts from ${feed.source} (${withGeo.length} with geometry)`);
      await saveAlerts(alerts);
    } else {
      console.log(`ℹ️ No valid alerts found in ${feed.source}`);
    }
  } catch (err) {
    console.error(`❌ Error fetching ${feed.name}:`, err.message);
  }
}

/** Poll all feeds */
export async function pollCapFeeds() {
  console.log("🚨 CAP Poller running...");
  for (const feed of CAP_FEEDS) await fetchCapFeed(feed);
  console.log("✅ CAP poll cycle complete.\n");
}