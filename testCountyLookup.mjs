import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { tryCountyCenterFromAreaDesc } from "./src/services/capPoller.mjs"; // <-- adjust if not exported

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const countyCentersPath = path.resolve(__dirname, "./src/data/county_centers.json");
const countyCenters = JSON.parse(fs.readFileSync(countyCentersPath, "utf8"));

const testAreas = [
  "Horry, SC; Marion, SC",
  "Lake, FL; Volusia, FL",
  "Brown, SD; Spink, SD",
  "Montgomery, AL"
];

for (const areaDesc of testAreas) {
  const result = (() => {
    const stateInDesc = (areaDesc.match(/\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR|GU|AS|MP|VI)\b/) || [])[0];
    const regions = areaDesc.split(/[,;]+/).map(s => s.trim());
    for (const r of regions) {
      let m = r.match(/^(.+?),\s*([A-Z]{2})$/);
      if (m) {
        const county = m[1];
        const st = m[2];
        if (countyCenters[st]?.[county]) return countyCenters[st][county];
      }
      if (stateInDesc && /^[A-Za-z .'-]+$/.test(r)) {
        if (countyCenters[stateInDesc]?.[r]) return countyCenters[stateInDesc][r];
      }
    }
    return null;
  })();

  console.log(`${areaDesc} → ${JSON.stringify(result)}`);
}
