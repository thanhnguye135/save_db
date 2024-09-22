const elasticClient = require("./elasticClient");

async function createIndexElasticSearch(data) {
  const indexName = "search";

  const body = data.flatMap((doc) => [{ index: { _index: indexName } }, doc]);

  try {
    const { body: bulkResponse } = await elasticClient.bulk({
      refresh: true,
      body,
    });

    if (bulkResponse && bulkResponse.errors) {
      const errorDetails = bulkResponse.items
        .filter((item) => item.index && item.index.error)
        .map((item) => item.index.error);
      throw new Error(`Bulk index errors: ${JSON.stringify(errorDetails)}`);
    }

    console.log(
      `Successfully indexed ${data.length} documents into ${indexName}`
    );
  } catch (error) {
    console.error(`Error indexing data to Elasticsearch: ${error.message}`);
    console.error("Data that caused the error:", JSON.stringify(data, null, 2));
  }
}

module.exports = { indexDataToElasticsearch: createIndexElasticSearch };
