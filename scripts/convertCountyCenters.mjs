import fs from "fs";

const input = JSON.parse(fs.readFileSync("./src/data/county_centers_raw.json", "utf8"));
const output = {};

for (const [state, counties] of Object.entries(input)) {
  const stateCenters = Object.values(counties);
  const avgLon = stateCenters.reduce((sum, c) => sum + c[0], 0) / stateCenters.length;
  const avgLat = stateCenters.reduce((sum, c) => sum + c[1], 0) / stateCenters.length;

  output[state] = {
    __center: [Number(avgLon.toFixed(4)), Number(avgLat.toFixed(4))],
    counties: {}
  };

  for (const [county, coords] of Object.entries(counties)) {
    output[state].counties[county] = { center: coords };
  }
}

fs.writeFileSync("./src/data/county_centers.json", JSON.stringify(output, null, 2));
console.log("✅ Reformatted county centers saved to src/data/county_centers.json");
