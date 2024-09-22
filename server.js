const express = require("express");
const bodyParser = require("body-parser");
const { findAllTransaction, findTransactionsByAttr } = require("./utils");

const app = express();
app.use(bodyParser.json());

app.use("/all-transactions", async (req, res) => {
  const { page, size } = req.query;
  const transactions = await findAllTransaction(page, size);
  res.json(transactions);
});

app.use("/transactions", async (req, res) => {
  const query = req.query;
  const transactions = await findTransactionsByAttr(query);
  res.json(transactions);
});

const port = 3000;
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
