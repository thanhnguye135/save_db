const fs = require("fs");
const csv = require("csv-parser");
const { Queue } = require("bullmq");
const redisConnection = require("./redis-connection");

const BATCH_QUEUE_NAME = "transactionBatchQueue";
const CHUNK_SIZE = 10000;

const batchQueue = new Queue(BATCH_QUEUE_NAME, { connection: redisConnection });

const addBatchToQueue = async (batch, batchId) => {
  try {
    await batchQueue.add("batchProcess", batch, { jobId: batchId });
  } catch (error) {
    console.error("Failed to add batch to queue:", error);
  }
};

const parseAndSaveCsv = (filePath) => {
  let records = [];
  let stream = fs.createReadStream(filePath).pipe(csv());

  stream
    .on("data", async (data) => {
      try {
        let keys = Object.keys(data);
        records.push({
          transactionDate: formatDate(data[keys[0]].split("_")[0]),
          docNumber: data[keys[0]].split("_")[1],
          debit: data[keys[3]],
          credit: data[keys[2]],
          vnd: formatVnd(data[keys[2]]),
          balance: 0,
          transactionDetails: data[keys[4]],
        });
        keys = null;
        if (records.length >= CHUNK_SIZE) {
          let batch = records.splice(0, CHUNK_SIZE);
          const batchId = `batch_${Date.now()}_${Math.random()}`;
          await addBatchToQueue(batch, batchId);
          batch = null;
        }
      } catch (error) {
        console.error("Error processing record:", error);
      }
    })
    .on("end", async () => {
      try {
        if (records.length > 0) {
          await addBatchToQueue(records); // Process remaining records
        }
        console.log("CSV file successfully processed.");
      } catch (error) {
        console.error("Error processing remaining records:", error);
      } finally {
        records = []; // Clear the reference
        stream = null; // Clear the stream reference
      }
    })
    .on("error", (err) => {
      console.error("Error reading the file:", err);
    });
};

const formatVnd = (amount) => {
  if (typeof amount !== "number") {
    amount = parseFloat(amount);
  }
  return amount.toLocaleString("vi-VN");
};

const formatDate = (date) => {
  const [day, month, year] = date.split("/");
  const dateObject = new Date(Date.UTC(year, month - 1, day, 0, 0, 0));

  return dateObject.toISOString();
};

parseAndSaveCsv("/home/monochromatic/Downloads/chuyen_khoan.csv");
// parseAndSaveCsv("/home/monochromatic/random_transactions_corrected.csv");
// parseAndSaveCsv("/home/monochromatic/two_m_records.csv");
// parseAndSaveCsv("/home/monochromatic/seven_m_records.csv");
// parseAndSaveCsv("/home/monochromatic/fake_data.csv");
