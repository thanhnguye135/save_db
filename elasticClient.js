require("dotenv").config();
const { Client } = require("@elastic/elasticsearch");

const elasticClient = new Client({
  node: process.env.ELASTICSEARCH_URL,
  auth: {
    username: process.env.ELASTICSEARCH_USERNAME,
    password: process.env.ELASTICSEARCH_PASSWORD,
  },
});

async function checkConnection() {
  try {
    const response = await elasticClient.ping();
    console.log("Elasticsearch connection is successful:", response);
  } catch (error) {
    console.error("Elasticsearch connection failed:", error);
  }
}

module.exports = { elasticClient };

checkConnection();
