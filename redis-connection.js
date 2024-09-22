const Redis = require("ioredis");

const redisConfig = {
  host: process.env.HOST_REDIS,
  port: process.env.PORT_REDIS,
  maxRetriesPerRequest: null,
};

const redisConnection = new Redis(redisConfig);

module.exports = redisConnection;
