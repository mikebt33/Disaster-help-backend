import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";

/**
 * CAP Alert Poller Service
 * Fetches, parses, and saves official CAP alerts (NOAA, NWS, FEMA, USGS, etc.)
 */

const CAP_FEEDS = [
  {
    name: "NWS National Feed",
    url: "https://alerts.weather.gov/cap/us.php?x=0",
    source: "NWS"
  },
  {
    name: "FEMA IPAWS",
    url: "https://ipaws.nws.noaa.gov/feeds/IPAWSOpenCAP.xml",
    source: "FEMA"
  },
  {
    name: "USGS Earthquakes",
    url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.atom",
    source: "USGS"
  }
];

/**
 * Converts polygon strings like "27.82,-82.78 27.85,-82.69 27.88,-82.74"
 * into valid GeoJSON coordinates
 */
function parsePolygon(polygonString) {
  if (!polygonString) return null;
  try {
    const coords = polygonString
      .trim()
      .split(" ")
      .map((p) => {
        const [lat, lon] = p.split(",");
        return [parseFloat(lon), parseFloat(lat)];
      });
    // Close the polygon if not closed
    if (coords.length > 2 && coords[0] !== coords[coords.length - 1]) {
      coords.push(coords[0]);
    }
    return { type: "Polygon", coordinates: [coords] };
  } catch {
    return null;
  }
}

/**
 * Parses one CAP alert entry into a standardized document
 */
function normalizeCapAlert(entry, source) {
  try {
    const info = Array.isArray(entry.info) ? entry.info[0] : entry.info;
    const area = info?.area || {};
    const polygon = area?.polygon || info?.polygon;
    const geometry = parsePolygon(polygon);

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
        instruction: info?.instruction || ""
      },
      area: {
        areaDesc: area?.areaDesc || info?.areaDesc || "",
        polygon: polygon || null
      },
      geometry,
      source,
      timestamp: new Date()
    };
  } catch (err) {
    console.error("⚠️ Error normalizing CAP alert:", err.message);
    return null;
  }
}

/**
 * Saves CAP alerts into alerts_cap collection
 * and mirrors polygon alerts into hazards
 */
async function saveAlerts(alerts) {
  const db = getDB();
  const alertsCol = db.collection("alerts_cap");
  const hazardsCol = db.collection("hazards");

  for (const alert of alerts) {
    if (!alert.identifier) continue;

    // Upsert by identifier (avoid duplicates)
    await alertsCol.updateOne(
      { identifier: alert.identifier },
      { $set: alert },
      { upsert: true }
    );

    // Mirror polygons into hazards
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
            timestamp: new Date()
          }
        },
        { upsert: true }
      );
    }
  }
}

/**
 * Fetches and processes a single feed URL
 */
async function fetchCapFeed(feed) {
  console.log(`🌐 Fetching ${feed.name} (${feed.source})`);
  try {
    const res = await axios.get(feed.url);
    const xml = res.data;

    const parser = new XMLParser({
      ignoreAttributes: false,
      removeNSPrefix: true
    });

    const json = parser.parse(xml);
    const entries = json.alert
      ? [json.alert]
      : json.feed?.entry || json.alerts || [];

    const alerts = entries
      .map((entry) => normalizeCapAlert(entry, feed.source))
      .filter(Boolean);

    if (alerts.length) {
      console.log(`✅ Parsed ${alerts.length} alerts from ${feed.source}`);
      await saveAlerts(alerts);
    } else {
      console.log(`ℹ️ No valid alerts found in ${feed.source}`);
    }
  } catch (err) {
    console.error(`❌ Error fetching ${feed.name}:`, err.message);
  }
}

/**
 * Poll all feeds
 */
export async function pollCapFeeds() {
  console.log("🚨 CAP Poller running...");
  for (const feed of CAP_FEEDS) {
    await fetchCapFeed(feed);
  }
  console.log("✅ CAP poll cycle complete.\n");
}
