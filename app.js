const fs = require("fs");
const csv = require("csv-parser");
const { Queue } = require("bullmq");
const redisConnection = require("./redis-connection");

const BATCH_QUEUE_NAME = "transactionBatchQueue";
const CHUNK = 20000;

const batchQueue = new Queue(BATCH_QUEUE_NAME, { connection: redisConnection });

const addBatchToQueue = async (batch) => {
  await batchQueue.add("batchProcess", batch);
  batch = null;
};

const parseAndSaveCsv = (filePath) => {
  let records = [];

  console.time("saved db");
  let stream = fs.createReadStream(filePath).pipe(csv());

  stream
    .on("data", async (data) => {
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

      keys = [];

      if (records.length >= CHUNK) {
        let batch = records;
        records = [];
        await addBatchToQueue(batch);
      }
    })
    .on("end", async () => {
      if (records.length > 0) {
        await addBatchToQueue(records);
      }
      records = null;
      stream = null;
      console.timeEnd("saved db");
      console.log("CSV file successfully processed.");
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

const formatDate = (isoDate) => {
  const date = new Date(isoDate);
  return new Intl.DateTimeFormat("en-US").format(date);
};

// parseAndSaveCsv("/home/monochromatic/Downloads/chuyen_khoan.csv");
// parseAndSaveCsv("./random_transactions_corrected.csv");
// parseAndSaveCsv("/home/monochromatic/two_m_records.csv");
parseAndSaveCsv("/home/monochromatic/fake_data.csv");
