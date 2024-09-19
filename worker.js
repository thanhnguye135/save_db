const { Worker } = require("bullmq");
const mongoose = require("mongoose");
const redisConnection = require("./redis-connection");

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

// Use reconnect strategy to handle disconnections
mongoose.connection.on("error", (error) => {
  console.error("Mongoose connection error: ", error);
  // Attempt to reconnect after 5 seconds
  setTimeout(() => connectToMongoDB(), 5000);
});

const BATCH_QUEUE_NAME = "transactionBatchQueue";

const batchWorker = new Worker(
  BATCH_QUEUE_NAME,
  async (job) => {
    const batch = job.data;
    try {
      // Insert records with unordered option to speed up insertion
      await Transaction.insertMany(batch, { ordered: false });
      console.log(`Inserted batch of ${batch.length} records.`);
    } catch (err) {
      console.error("Error inserting batch:", err);
    }
  },
  {
    connection: redisConnection,
    concurrency: 5, // Adjust based on your needs and system capacity
  }
);

batchWorker.on("failed", (job, error) => {
  console.error(`Job ${job.id} failed: `, error);
});

batchWorker.on("completed", (job) => {
  console.log(`Job ${job.id} completed successfully`);
});

console.log("Worker is running...");
