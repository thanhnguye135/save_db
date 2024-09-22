const { Worker } = require("bullmq");
const mongoose = require("mongoose");
const cluster = require("cluster");
const os = require("os");
const redisConnection = require("./redis-connection");
const { elasticClient } = require("./elasticClient");

const numCPUs = os.cpus().length;

const TOTAL_TIME_KEY = "totalTime";

const addToTotalTime = async (time) => {
  await redisConnection.incrby(TOTAL_TIME_KEY, time);
};

if (cluster.isMaster) {
  console.log(`Master process ${process.pid} is running`);

  // Fork worker processes
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Spawning a new one.....`);
    cluster.fork();
  });
} else {
  const transactionSchema = new mongoose.Schema({
    transactionDate: Date,
    docNumber: String,
    debit: Number,
    credit: Number,
    vnd: String,
    balance: Number,
    transactionDetails: String,
  });

  const Transaction = mongoose.model("Transaction1", transactionSchema);

  // Connect to MongoDB
  const connectToMongoDB = async () => {
    try {
      await mongoose.connect("mongodb://localhost:27017/transaction");
      console.log("Mongoose connected to MongoDB");
    } catch (error) {
      console.error("Mongoose connection error: ", error);
    }
  };

  connectToMongoDB();

  mongoose.connection.on("disconnected", () => {
    console.log("Mongoose disconnected");
  });

  mongoose.connection.on("error", (error) => {
    console.error("Mongoose connection error: ", error);
    // Attempt to reconnect after 5 seconds
    setTimeout(connectToMongoDB, 5000);
  });

  const BATCH_QUEUE_NAME = "transactionBatchQueue";

  // Worker processing batches of transactions
  batchWorker = new Worker(
    BATCH_QUEUE_NAME,
    async (job) => {
      const batch = job.data;

      try {
        const startTime = Date.now();
        await Transaction.insertMany(batch);
        const time = Date.now() - startTime;
        await addToTotalTime(time);
        console.log(`Inserted batch of ${batch.length} records in ${time} ms.`);

        let bulkOperations = [];
        batch.forEach((transaction) => {
          bulkOperations.push(
            {
              index: {
                _index: "transactions",
              },
            },
            {
              transactionDate: transaction.transactionDate,
              docNumber: transaction.docNumber,
              debit: transaction.debit,
              credit: transaction.credit,
              vnd: transaction.vnd,
              balance: transaction.balance,
              transactionDetails: transaction.transactionDetails,
            }
          );
        });

        if (bulkOperations.length > 0) {
          const esResult = await elasticClient.bulk({
            refresh: false,
            body: bulkOperations,
          });

          if (esResult.errors) {
            const failedItems = esResult.items.filter(
              (item) => item.index && item.index.error
            );
            console.error(
              `Some records failed to index in Elasticsearch:`,
              failedItems
            );
          }
        } else {
          console.error("No operations to send to Elasticsearch.");
        }
      } catch (err) {
        console.error(`Error inserting batch for job ${job.id}:`, err);
      }
    },
    {
      connection: redisConnection,
      concurrency: 10,
    }
  );

  batchWorker.on("failed", (job, error) => {
    console.error(`Job ${job.id} failed: `, error);
  });

  batchWorker.on("completed", (job) => {
    console.log(`Job ${job.id} completed successfully`);
  });

  console.log(`Worker ${process.pid} is running...`);
}
