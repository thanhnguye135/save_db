const Redis = require("ioredis");

const redisConfig = {
  host: "127.0.0.1",
  port: 6379,
  maxRetriesPerRequest: null,
};

const redisConnection = new Redis(redisConfig);

module.exports = redisConnection;
