import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";

/**
 * CAP Alert Poller Service
 * Adds automatic geometry/centroid fallback for polygon-only alerts.
 * Expanded to all U.S. states, territories, and national feeds.
 */

const CAP_FEEDS = [
  // 🌐 National + Federal Feeds
  { name: "NWS National Feed", url: "https://alerts.weather.gov/cap/us.php?x=0", source: "NWS" },
  { name: "FEMA IPAWS", url: "https://ipaws.nws.noaa.gov/feeds/IPAWSOpenCAP.xml", source: "FEMA" },
  { name: "USGS Earthquakes", url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.atom", source: "USGS" },

  // 🇺🇸 50 States + DC + Territories
  { name: "Alabama NWS Feed", url: "https://alerts.weather.gov/cap/al.php?x=0", source: "NWS" },
  { name: "Alaska NWS Feed", url: "https://alerts.weather.gov/cap/ak.php?x=0", source: "NWS" },
  { name: "Arizona NWS Feed", url: "https://alerts.weather.gov/cap/az.php?x=0", source: "NWS" },
  { name: "Arkansas NWS Feed", url: "https://alerts.weather.gov/cap/ar.php?x=0", source: "NWS" },
  { name: "California NWS Feed", url: "https://alerts.weather.gov/cap/ca.php?x=0", source: "NWS" },
  { name: "Colorado NWS Feed", url: "https://alerts.weather.gov/cap/co.php?x=0", source: "NWS" },
  { name: "Connecticut NWS Feed", url: "https://alerts.weather.gov/cap/ct.php?x=0", source: "NWS" },
  { name: "Delaware NWS Feed", url: "https://alerts.weather.gov/cap/de.php?x=0", source: "NWS" },
  { name: "District of Columbia NWS Feed", url: "https://alerts.weather.gov/cap/dc.php?x=0", source: "NWS" },
  { name: "Florida NWS Feed", url: "https://alerts.weather.gov/cap/fl.php?x=0", source: "NWS" },
  { name: "Georgia NWS Feed", url: "https://alerts.weather.gov/cap/ga.php?x=0", source: "NWS" },
  { name: "Hawaii NWS Feed", url: "https://alerts.weather.gov/cap/hi.php?x=0", source: "NWS" },
  { name: "Idaho NWS Feed", url: "https://alerts.weather.gov/cap/id.php?x=0", source: "NWS" },
  { name: "Illinois NWS Feed", url: "https://alerts.weather.gov/cap/il.php?x=0", source: "NWS" },
  { name: "Indiana NWS Feed", url: "https://alerts.weather.gov/cap/in.php?x=0", source: "NWS" },
  { name: "Iowa NWS Feed", url: "https://alerts.weather.gov/cap/ia.php?x=0", source: "NWS" },
  { name: "Kansas NWS Feed", url: "https://alerts.weather.gov/cap/ks.php?x=0", source: "NWS" },
  { name: "Kentucky NWS Feed", url: "https://alerts.weather.gov/cap/ky.php?x=0", source: "NWS" },
  { name: "Louisiana NWS Feed", url: "https://alerts.weather.gov/cap/la.php?x=0", source: "NWS" },
  { name: "Maine NWS Feed", url: "https://alerts.weather.gov/cap/me.php?x=0", source: "NWS" },
  { name: "Maryland NWS Feed", url: "https://alerts.weather.gov/cap/md.php?x=0", source: "NWS" },
  { name: "Massachusetts NWS Feed", url: "https://alerts.weather.gov/cap/ma.php?x=0", source: "NWS" },
  { name: "Michigan NWS Feed", url: "https://alerts.weather.gov/cap/mi.php?x=0", source: "NWS" },
  { name: "Minnesota NWS Feed", url: "https://alerts.weather.gov/cap/mn.php?x=0", source: "NWS" },
  { name: "Mississippi NWS Feed", url: "https://alerts.weather.gov/cap/ms.php?x=0", source: "NWS" },
  { name: "Missouri NWS Feed", url: "https://alerts.weather.gov/cap/mo.php?x=0", source: "NWS" },
  { name: "Montana NWS Feed", url: "https://alerts.weather.gov/cap/mt.php?x=0", source: "NWS" },
  { name: "Nebraska NWS Feed", url: "https://alerts.weather.gov/cap/ne.php?x=0", source: "NWS" },
  { name: "Nevada NWS Feed", url: "https://alerts.weather.gov/cap/nv.php?x=0", source: "NWS" },
  { name: "New Hampshire NWS Feed", url: "https://alerts.weather.gov/cap/nh.php?x=0", source: "NWS" },
  { name: "New Jersey NWS Feed", url: "https://alerts.weather.gov/cap/nj.php?x=0", source: "NWS" },
  { name: "New Mexico NWS Feed", url: "https://alerts.weather.gov/cap/nm.php?x=0", source: "NWS" },
  { name: "New York NWS Feed", url: "https://alerts.weather.gov/cap/ny.php?x=0", source: "NWS" },
  { name: "North Carolina NWS Feed", url: "https://alerts.weather.gov/cap/nc.php?x=0", source: "NWS" },
  { name: "North Dakota NWS Feed", url: "https://alerts.weather.gov/cap/nd.php?x=0", source: "NWS" },
  { name: "Ohio NWS Feed", url: "https://alerts.weather.gov/cap/oh.php?x=0", source: "NWS" },
  { name: "Oklahoma NWS Feed", url: "https://alerts.weather.gov/cap/ok.php?x=0", source: "NWS" },
  { name: "Oregon NWS Feed", url: "https://alerts.weather.gov/cap/or.php?x=0", source: "NWS" },
  { name: "Pennsylvania NWS Feed", url: "https://alerts.weather.gov/cap/pa.php?x=0", source: "NWS" },
  { name: "Rhode Island NWS Feed", url: "https://alerts.weather.gov/cap/ri.php?x=0", source: "NWS" },
  { name: "South Carolina NWS Feed", url: "https://alerts.weather.gov/cap/sc.php?x=0", source: "NWS" },
  { name: "South Dakota NWS Feed", url: "https://alerts.weather.gov/cap/sd.php?x=0", source: "NWS" },
  { name: "Tennessee NWS Feed", url: "https://alerts.weather.gov/cap/tn.php?x=0", source: "NWS" },
  { name: "Texas NWS Feed", url: "https://alerts.weather.gov/cap/tx.php?x=0", source: "NWS" },
  { name: "Utah NWS Feed", url: "https://alerts.weather.gov/cap/ut.php?x=0", source: "NWS" },
  { name: "Vermont NWS Feed", url: "https://alerts.weather.gov/cap/vt.php?x=0", source: "NWS" },
  { name: "Virginia NWS Feed", url: "https://alerts.weather.gov/cap/va.php?x=0", source: "NWS" },
  { name: "Washington NWS Feed", url: "https://alerts.weather.gov/cap/wa.php?x=0", source: "NWS" },
  { name: "West Virginia NWS Feed", url: "https://alerts.weather.gov/cap/wv.php?x=0", source: "NWS" },
  { name: "Wisconsin NWS Feed", url: "https://alerts.weather.gov/cap/wi.php?x=0", source: "NWS" },
  { name: "Wyoming NWS Feed", url: "https://alerts.weather.gov/cap/wy.php?x=0", source: "NWS" },
  { name: "Puerto Rico NWS Feed", url: "https://alerts.weather.gov/cap/pr.php?x=0", source: "NWS" },
  { name: "Guam NWS Feed", url: "https://alerts.weather.gov/cap/gu.php?x=0", source: "NWS" },
  { name: "American Samoa NWS Feed", url: "https://alerts.weather.gov/cap/as.php?x=0", source: "NWS" },
  { name: "Northern Mariana Islands NWS Feed", url: "https://alerts.weather.gov/cap/mp.php?x=0", source: "NWS" },
  { name: "U.S. Virgin Islands NWS Feed", url: "https://alerts.weather.gov/cap/vi.php?x=0", source: "NWS" },
];

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

/** Compute centroid from a GeoJSON Polygon */
function polygonCentroid(geometry) {
  if (!geometry || geometry.type !== "Polygon") return null;
  const coords = geometry.coordinates[0];
  let x = 0, y = 0, z = 0;
  for (const [lon, lat] of coords) {
    const latR = (lat * Math.PI) / 180, lonR = (lon * Math.PI) / 180;
    x += Math.cos(latR) * Math.cos(lonR);
    y += Math.cos(latR) * Math.sin(lonR);
    z += Math.sin(latR);
  }
  const total = coords.length;
  x /= total; y /= total; z /= total;
  const lon = Math.atan2(y, x);
  const hyp = Math.sqrt(x * x + y * y);
  const lat = Math.atan2(z, hyp);
  return { type: "Point", coordinates: [lon * (180 / Math.PI), lat * (180 / Math.PI)] };
}

/** Normalize a single CAP alert entry */
function normalizeCapAlert(entry, source) {
  try {
    const info = Array.isArray(entry.info) ? entry.info[0] : entry.info;
    const area = info?.area || {};
    const polygon = area?.polygon || info?.polygon;
    const polygonGeom = parsePolygon(polygon);
    let geometry = polygonGeom ? polygonCentroid(polygonGeom) : null;

    // Fallback to explicit lat/lon if present
    const lat = parseFloat(info?.lat || info?.latitude || area?.lat) || null;
    const lon = parseFloat(info?.lon || info?.longitude || area?.lon) || null;
    if (!geometry && lat && lon)
      geometry = { type: "Point", coordinates: [lon, lat] };

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
        polygon: polygon || null,
      },
      geometry,
      source,
      timestamp: new Date(),
    };
  } catch (err) {
    console.error("⚠️ Error normalizing CAP alert:", err.message);
    return null;
  }
}

/** Save CAP alerts into MongoDB (and mirror to hazards) */
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

/** Fetch and process a feed */
async function fetchCapFeed(feed) {
  console.log(`🌐 Fetching ${feed.name} (${feed.source})`);
  try {
    const res = await axios.get(feed.url);
    const xml = res.data;
    const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
    const json = parser.parse(xml);
    const entries = json.alert ? [json.alert] : json.feed?.entry || json.alerts || [];
    const alerts = entries.map((e) => normalizeCapAlert(e, feed.source)).filter(Boolean);
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

/** Poll all feeds */
export async function pollCapFeeds() {
  console.log("🚨 CAP Poller running...");
  for (const feed of CAP_FEEDS) await fetchCapFeed(feed);
  console.log("✅ CAP poll cycle complete.\n");
}
