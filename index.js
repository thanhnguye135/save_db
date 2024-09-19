const mongodb = require("mongodb");
const IORedis = require("ioredis");
const { Queue, Worker } = require("bullmq");
const cpus = require("os").cpus;
const cluster = require("cluster");
const csv = require("csv-parser");
const fs = require("fs");

const redisConnection = new IORedis({
  host: "localhost",
  port: 6379,
  db: 5,
  maxRetriesPerRequest: null,
});

let urlDB = "mongodb://localhost:27017/filecsv?authSource=admin";

function formatCurrency(value) {
  let number = parseFloat(value);
  if (isNaN(number)) return value;
  return number
    .toLocaleString("vi-VN", { style: "currency", currency: "VND" })
    .replace("₫", "");
}

const numCPUs = cpus().length;
console.log(numCPUs);

const importQueue = new Queue("csvImportQueue", {
  connection: redisConnection,
});

let totalExecutionTime = 0;

if (cluster.isMaster) {
  console.log(`Master process ${process.pid} is running`);

  const startTime = Date.now();

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Forking a new one...`);
    cluster.fork();
  });

  async function addChunkJobsToQueue(fileName) {
    const CHUNK_SIZE = 500;
    let chunkData = [];
    let chunkCount = 0;

    const readStream = fs.createReadStream(fileName).pipe(csv());

    for await (const row of readStream) {
      chunkData.push(row);

      if (chunkData.length === CHUNK_SIZE) {
        await importQueue.add("importChunk", { chunkData });
        chunkCount++;
        chunkData = [];
      }
    }

    if (chunkData.length > 0) {
      await importQueue.add("importChunk", { chunkData });
      chunkCount++;
    }

    console.log(`${chunkCount} chunks added to queue.`);

    const endTime = Date.now();
    const totalTime = endTime - startTime;
    console.log(`Total import time: ${totalTime}ms`);
  }

  addChunkJobsToQueue("./random_transactions_corrected.csv");
} else {
  console.log(`Worker process ${process.pid} started`);

  const worker = new Worker(
    "csvImportQueue",
    async (job) => {
      const { chunkData } = job.data;
      await processChunk(chunkData);
      console.log(`Processed chunk job: ${job.id} by worker ${process.pid}`);
    },
    { connection: redisConnection }
  );

  async function processChunk(chunkData) {
    const startTime = Date.now();

    try {
      const client = await mongodb.MongoClient.connect(urlDB);
      const dbConn = client.db();
      const arrayToInsert = chunkData.map((row) => {
        let dateTimeParts = row[Object.keys(row)[0]].split("_");

        return {
          day: dateTimeParts[0],
          transactionId: dateTimeParts[1],
          credit: row["credit"],
          detail: row["detail"],
          VND: formatCurrency(row["credit"]),
        };
      });

      let collection = dbConn.collection("saoke3");
      const result = await collection.insertMany(arrayToInsert);

      if (result) {
        console.log("Chunk inserted into database successfully.");
      }

      await client.close();
    } catch (err) {
      console.error(`Error in worker ${process.pid}: ${err.message}`);
    } finally {
      const endTime = Date.now();
      const executionTime = endTime - startTime;
      totalExecutionTime += executionTime;
      console.log(`processChunk execution time: ${executionTime}ms`);
      console.log(`Total execution time so far: ${totalExecutionTime}ms`);
    }
  }
}
