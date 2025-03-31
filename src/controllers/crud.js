const { fetchAndStoreInitialData } = require("../query");
const { pool } = require("../db");
const { removeStockFromMemory, addStockData } = require("../memory");
const { upstoxWebsocket } = require("../websocket");

function handleWebSocketData(data) {
  if (data) {
    addStockData(data);
  }
}

const addStock = async (req, res) => {
  const { stock_symbol } = req.body;
  const stockSymbols = Array.isArray(stock_symbol)
    ? stock_symbol
    : [stock_symbol].filter(Boolean);

  if (!stockSymbols.length) return res.status(400).send("Missing stock_symbol in request body");

  try {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const addedStocks = [];
      for (const stock_symbol of stockSymbols) {
        const result = await client.query(
          `INSERT INTO stock_subscriptions (stock_symbol, subscribed_at) VALUES ($1, $2) ON CONFLICT (stock_symbol) DO NOTHING RETURNING *`,
          [stock_symbol, Date.now()]
        );
        if (result.rowCount > 0) addedStocks.push(stock_symbol);
      }

      if (addedStocks.length === 0) {
        await client.query("ROLLBACK");
        return res
          .status(409)
          .send("All requested stocks are already subscribed");
      }

      await client.query("COMMIT");

      for (const stock_symbol of addedStocks) {
        await fetchAndStoreInitialData(stock_symbol);
        upstoxWebsocket.subscribe(stock_symbol, handleWebSocketData);
      }

      res.status(200).send(`Subscribed to ${addedStocks.join(", ")}`);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("Error adding stock subscriptions:", e.message);
    res.status(500).send("Internal server error");
  }
};

const removeStock = async (req, res) => {
  const { stock_symbol } = req.body;
  if (!stock_symbol) return res.status(400).send("Missing stock_symbol in request body");

  try {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const subResult = await client.query(
        `DELETE FROM stock_subscriptions WHERE stock_symbol = $1 RETURNING *`,
        [stock_symbol]
      );
      if (subResult.rowCount === 0) {
        await client.query("ROLLBACK");
        return res.status(404).send(`Stock ${stock_symbol} not subscribed`);
      }

      const aggResult = await client.query(
        `DELETE FROM stock_aggregates WHERE stock_symbol = $1 RETURNING *`,
        [stock_symbol]
      );

      const stocksResult = await pool.query(
        `DELETE FROM stocks WHERE stock_symbol = $1 RETURNING *`,
        [stock_symbol]
      );

      await client.query("COMMIT");

      upstoxWebsocket.unsubscribe(stock_symbol);
      removeStockFromMemory(stock_symbol);

      res.send(
        `Unsubscribed from ${stock_symbol}, removed ${aggResult.rowCount} aggregation records, ${stocksResult.rowCount} stock records`
      );
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("Error removing stock subscription:", e.message);
    res.status(500).send("Internal server error");
  }
};

module.exports = {
  addStock,
  removeStock,
};
