import { Client } from "@elastic/elasticsearch";
import models from "database";
import esConfig from "../src/config/esConfig";
import esConfigQueries from "../src/config/esConfigQueries";
import logger from "./logger";
import commonService from "../src/helpers/services";
const elasticClient = new Client({ node: `http://localhost:9200` });

const bulkDocIndexing = async () => {
  const resObj = {};
  const tableNames = Object.keys(esConfig);
  await Promise.all(
    tableNames.map(async table => {
      try {
        const singularizeTableName = table.trim().slice(0, table.length - 1);
        const query = `select id,${
          esConfig[table].props
        } from ${table} where status="active"`;

        const res = await commonService.findByQuery({
          Model: models[singularizeTableName],
          rawQuery: query,
          options: {
            sortBy: ["skip"]
          },
          autoFormat: false
        });

        resObj[esConfig[table].index] = res;
        return res;
      } catch (err) {
        logger.error(err);
      }
    })
  );
  const promiseDocs = [];
  try {
    tableNames.forEach(async table => {
      resObj[esConfig[table].index].forEach(async spr => {
        const insertedDoc = await elasticClient.index({
          id: spr.id,
          index: esConfig[table].index,
          body: spr
        });

        promiseDocs.push(insertedDoc);
      });
    });
    await Promise.all(promiseDocs);
  } catch (err) {
    logger.error(err);
  }

  logger.info("Documents inserted into the indices successfully");
};

const createBulkIndices = async () => {
  const tableNames = Object.keys(esConfig);
  await Promise.all(
    tableNames.map(async table => {
      try {
        await elasticClient.indices.create({
          index: esConfig[table].index,
          body: {
            mappings: esConfig[table].mappings
            // settings: esConfig[table].settings,
          }
        });
      } catch (err) {
        logger.error(err);
      }
    })
  );

  // creating esConfigQueries indices
  const tableNamesforQueries = Object.keys(esConfigQueries);
  await Promise.all(
    tableNamesforQueries.map(async table => {
      try {
        await elasticClient.indices.create({
          index: esConfigQueries[table].index,
          body: {
            mappings: esConfigQueries[table].mappings,
            settings: esConfigQueries[table].settings
          }
        });
      } catch (err) {
        logger.error(err);
      }
    })
  );

  logger.info("indices created successfully");
};

const docByQuery = async () => {
  const resObj = {};
  const tableNames = Object.keys(esConfigQueries);
  await Promise.all(
    tableNames.map(async table => {
      try {
        const singularizeTableName = table.trim().slice(0, table.length - 1);
        const { query } = esConfigQueries[table];

        const res = await commonService.findByQuery({
          Model: models[singularizeTableName],
          rawQuery: query,
          options: {
            sortBy: ["skip"]
          },
          autoFormat: false
        });

        resObj[esConfigQueries[table].index] = res;
        return res;
      } catch (err) {
        logger.error(err);
      }
    })
  );

  const promiseDocs = [];
  try {
    tableNames.forEach(async table => {
      resObj[esConfigQueries[table].index].forEach(async spr => {
        const insertedDoc = await elasticClient.index({
          index: esConfigQueries[table].index,
          body: spr
        });

        promiseDocs.push(insertedDoc);
      });
    });
    await Promise.all(promiseDocs);
  } catch (err) {
    logger.error(err);
  }
  logger.info("Query docs added successfully");
};

const search = async query => {
  let res;
  const { s } = query;

  try {
    res = await elasticClient.search({
      index: [""],
      body: {
        query: {
          bool: {
            should: [{ match: { name: s } }, { match: { location: s } }]
          }
        }
      }
    });
  } catch (err) {
    logger.error(err);
  }
  return res && res.body;
};

async function addDoc({ id, indexDoc, index }) {
  try {
    await elasticClient.index({
      id,
      index,
      body: indexDoc
    });
  } catch (err) {
    logger.error(err);
  }
  logger.info("Document added successfully");
}

export default {
  bulkDocIndexing,
  createBulkIndices,
  docByQuery,
  search,
  addDoc
};
