const { elasticClient } = require("./elasticClient");

async function findAllTransaction(page = 0, size = 100) {
  try {
    const result = await elasticClient.search({
      index: "transactions",
      query: { match_all: {} },
      from: page * size,
      size: size,
    });

    return result.hits.hits.map((hit) => hit._source);
  } catch (error) {
    console.error("Error fetching transactions from elasticSearch", error);
    throw error;
  }
}

async function findTransactionsByAttr(query) {
  const filters = {
    bool: {
      must: [],
    },
  };

  if (query.transactionDate) {
    filters.bool.must.push({
      match: { transactionDate: query.transactionDate },
    });
  }

  if (query.docNumber) {
    filters.bool.must.push({
      match: { docNumber: query.docNumber },
    });
  }

  if (query.debit) {
    filters.bool.must.push({
      match: { debit: query.debit },
    });
  }

  if (query.credit) {
    filters.bool.must.push({
      match: { credit: query.credit },
    });
  }

  if (query.vnd) {
    filters.bool.must.push({
      match: { vnd: query.vnd },
    });
  }

  if (query.balance) {
    filters.bool.must.push({
      match: { balance: query.balance },
    });
  }

  if (query.transactionDetails) {
    filters.bool.must.push({
      match_phrase: { transactionDetails: query.transactionDetails },
    });
  }

  try {
    const result = await elasticClient.search({
      index: "transactions",
      query: filters,
      from: query?.page || 0,
      size: query?.size || 100,
    });
    return result.hits.hits.map((hit) => hit._source);
  } catch (error) {
    console.log("Error searching transactions", error);
  }
}

module.exports = { findAllTransaction, findTransactionsByAttr };
