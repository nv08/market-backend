const { getStockData } = require("../websocket");
const { pool } = require("../db");
require("dotenv").config();
const { addStockData } = require("../memory");

async function fetchAndStoreInitialData(symbol) {
  const data = await getStockData(symbol);
  if (!data || !Array.isArray(data)) {
    console.error(`No valid data returned for ${symbol}`);
    return;
  }

  const now = Date.now();
  const fiveMinutesAgo = now - 5 * 60 * 1000; // 5 minutes ago in milliseconds
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Process all candles
    for (const candle of data) {
      const { timestamp, open, high, low, close, cmp } = candle;
      const stockData = {
        stock_symbol: symbol,
        cmp,
        open,
        high,
        low,
        close,
        timestamp,
      };

      // Store last 5 minutes in memory
      if (timestamp >= fiveMinutesAgo) {
        addStockData(stockData);
        console.log(
          `Added to memory (last 5 min): ${symbol}, ts: ${new Date(
            timestamp
          ).toISOString()}`
        );
      }

      // Store all data in DB (no conditions)
      await client.query(
        `INSERT INTO stocks (stock_symbol, cmp, open, high, low, close, timestamp) 
           VALUES ($1, $2, $3, $4, $5, $6, $7) 
           ON CONFLICT DO NOTHING`,
        [symbol, cmp, open, high, low, close, timestamp]
      );
    }

    await client.query("COMMIT");
    console.log(
      `Stored all initial data for ${symbol} in DB and last 5 minutes in memory`
    );
  } catch (e) {
    await client.query("ROLLBACK");
    console.error(`Error storing initial data for ${symbol}:`, e.message);
  } finally {
    client.release();
  }
}

module.exports = {
  fetchAndStoreInitialData,
};
