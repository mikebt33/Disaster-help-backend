import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import { getDB } from "../db.js";

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
  })),
];

/** Fallback centers for state-level alerts */
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
  US: [-98.5795, 39.8283],
};

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
  if (aOutsideLat !== bOutsideLat)
    return aOutsideLat > bOutsideLat ? "lonlat" : "latlon";
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

/* ------------------- Normalization ------------------- */

function normalizeCapAlert(entry,source){
  try{
    const root=entry["cap:alert"]||entry.alert||entry.content?.["cap:alert"]||entry;
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

    if(!geometry){
      const desc=area?.areaDesc||root?.areaDesc||"";
      const stateMatch=desc.match(/\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR|GU|AS|MP|VI)\b/);
      const state=stateMatch?.[1];
      const coords=STATE_CENTERS[state]||STATE_CENTERS.US;
      geometry={type:"Point",coordinates:coords};
      geometryMethod=state?"state-center":"us-default";
    }

    let bbox=null;
    if(polygonGeom?.coordinates?.[0]?.length>2){
      const pts=polygonGeom.coordinates[0];
      const lons=pts.map(p=>p[0]);
      const lats=pts.map(p=>p[1]);
      bbox=[Math.min(...lons),Math.min(...lats),Math.max(...lons),Math.max(...lats)];
    }

    // --- Human readable event / title ---
    let eventName =
      info?.event ||
      root?.event ||
      (root.title && root.title.split(" issued")[0]) ||
      (root.summary && root.summary.split(" issued")[0]) ||
      "Alert";

    // Label USGS alerts as Earthquake
    if (source === "USGS" && eventName === "Alert") eventName = "Earthquake";

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
    // Enhanced cleanup for USGS HTML
    descriptionText = descriptionText
      .replace(/<\/?(dl|dt|dd)>/g, " ")
      .replace(/<[^>]*>/g, " ")
      .replace(/&deg;/g, "°")
      .replace(/\s+/g, " ")
      .trim();

    let instructionText = info?.instruction || root?.instruction || "";
    if (typeof instructionText !== "string") {
      instructionText = String(instructionText ?? "");
    }

    const infoBlock = {
      category: info?.category || "General",
      event: eventName.trim(),
      urgency: info?.urgency || "Unknown",
      severity: info?.severity || "Unknown",
      certainty: info?.certainty || "Unknown",
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
      hasGeometry:geometryMethod!=="us-default",
      title:headlineText.trim(),
      summary:descriptionText.trim(),
      source,
      timestamp:new Date(),
      expires:info?.expires||root?.expires||null,
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
    const alerts=entries.map(e=>normalizeCapAlert(e,feed.source)).filter(Boolean);
    const usable=alerts.filter(a=>a.hasGeometry).length;
    console.log(`✅ Parsed ${alerts.length} alerts from ${feed.source} (${usable} usable geo)`);
    if(alerts.length)await saveAlerts(alerts);
  }catch(err){
    console.error(`❌ Error fetching ${feed.name}:`,err.message);
  }
}

/** Run all feeds */
async function pollCapFeeds(){
  console.log("🚨 CAP Poller running...");
  for(const feed of CAP_FEEDS)await fetchCapFeed(feed);
  console.log("✅ CAP poll cycle complete.\n");
}

export { pollCapFeeds };
