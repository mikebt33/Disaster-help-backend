const { MongoClient } = require("mongodb");

const uri = "mongodb+srv://app_user:Secure12345@cluster0.ut87h56.mongodb.net/disaster_help?authSource=admin&retryWrites=true&w=majority";

const client = new MongoClient(uri);

(async () => {
  try {
    await client.connect();
    console.log("✅ Successfully connected to MongoDB!");
  } catch (err) {
    console.error("❌ Connection failed:", err.message);
  } finally {
    await client.close();
  }
})();