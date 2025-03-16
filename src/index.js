"use strict";

const express = require("express");
const { setupDatabase, pool } = require("./db");
const {
  computePctChange,
  getLatestStock,
  removeStockFromMemory,
  addStockData,
} = require("./memory");
const { flushToDatabase, runAggregations } = require("./aggregator");
const { simulateWebSocketData, stopSimulation } = require("./simulator");
const cors = require("cors");
require("dotenv").config();
const { UpstoxWebSocket, getStockData } = require("./websocket");
const {
  FLUSHING_TO_DB_INTERVAL,
  RUNNING_AGGREGATIONS_INTERVAL,
  DEFAULT_TOP_STOCKS_LIMIT,
  DEFAULT_SORT_MINUTE,
} = require("./constants");

const app = express();
const port = process.env.PORT || 3000;
app.use(cors());
const token = process.env.UPSTOX_TOKEN;

// WebSocket callbacks
const onOpen = () => {
  console.log("WebSocket opened");
};

const onClose = () => {
  console.log("WebSocket closed");
};

const onError = (error) => {
  console.error("WebSocket error:", error);
};

const upstoxWebsocket = new UpstoxWebSocket(token, onOpen, onClose, onError);

function handleWebSocketData(data) {
  if (data && data.stock_symbol) {
    addStockData(data); // Add real-time WebSocket data to memory
  }
}

async function fetchAndStoreInitialData(symbol) {
  const data = await getStockData(symbol);
  if (!data || !Array.isArray(data.candles)) {
    console.error(`No valid data returned for ${symbol}`);
    return;
  }

  const now = Date.now();
  const fiveMinutesAgo = now - 5 * 60 * 1000;
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const candle of data.candles) {
      const { timestamp, open, high, low, close, cmp } = candle;
      if (timestamp < fiveMinutesAgo) {
        // Older data to DB
        await client.query(
          `
          INSERT INTO stocks (stock_symbol, cmp, open, high, low, close, timestamp)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT DO NOTHING
          `,
          [symbol, cmp, open, high, low, close, timestamp]
        );
      } else {
        // Recent data to memory
        addStockData({
          stock_symbol: symbol,
          cmp,
          open,
          high,
          low,
          close,
          timestamp,
        });
      }
    }

    await client.query("COMMIT");
    console.log(`Stored initial data for ${symbol}`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error(`Error storing initial data for ${symbol}:`, e.message);
  } finally {
    client.release();
  }
}

async function startServer() {
  await setupDatabase();
  setInterval(flushToDatabase, FLUSHING_TO_DB_INTERVAL); // Flush every 6 seconds
  setInterval(runAggregations, RUNNING_AGGREGATIONS_INTERVAL); // Aggregate every 1 minute

  try {
    const result = await pool.query(`
      SELECT stock_symbol FROM stock_subscriptions
    `);
    const subscriptions = result.rows;
    if (subscriptions.length === 0) {
      console.log("No stock subscriptions found at startup");
    } else {
      for (const { stock_symbol } of subscriptions) {
        await fetchAndStoreInitialData(stock_symbol);
        upstoxWebsocket.subscribe(stock_symbol, handleWebSocketData);
        // simulateWebSocketData(10, stock_symbol, 1);
      }
      console.log(
        `Initialized ${subscriptions.length} stocks with WebSocket subscriptions`
      );
    }
  } catch (e) {
    console.error("Error querying stock subscriptions:", e.message);
  }

  console.log("Server started");
}

// Routes

// Get latest stock data (from memory or DB, only subscribed stocks)
app.get("/stock/:symbol", async (req, res) => {
  const { symbol } = req.params;

  const subCheck = await pool.query(
    "SELECT 1 FROM stock_subscriptions WHERE stock_symbol = $1",
    [symbol]
  );
  if (subCheck.rowCount === 0) {
    return res.status(404).send(`Stock ${symbol} is not subscribed`);
  }

  const latest = getLatestStock(symbol);
  if (latest) {
    res.json(latest);
  } else {
    try {
      const result = await pool.query(
        `
        SELECT * FROM stocks
        WHERE stock_symbol = $1
        ORDER BY timestamp DESC
        LIMIT 1
        `,
        [symbol]
      );
      res.json(result.rows[0] || { error: "Stock not found" });
    } catch (e) {
      console.error("Error fetching stock:", e.message);
      res.status(500).send("Internal server error");
    }
  }
});

// Get percentage change for a specific interval
app.get("/stock/:symbol/minute-changes/:minutes", async (req, res) => {
  const { symbol, minutes } = req.params;
  const minutesInt = parseInt(minutes, 10);

  const subCheck = await pool.query(
    "SELECT 1 FROM stock_subscriptions WHERE stock_symbol = $1",
    [symbol]
  );
  if (subCheck.rowCount === 0) {
    return res.status(404).send(`Stock ${symbol} is not subscribed`);
  }

  if (isNaN(minutesInt) || minutesInt <= 0) {
    return res.status(400).send("Invalid minutes value");
  }

  if (minutesInt <= 5) {
    const pctChange = computePctChange(symbol, minutesInt);
    if (pctChange !== null) {
      res.json({ interval_minutes: minutesInt, pct_change: pctChange });
    } else {
      res.status(404).send("Insufficient data in memory");
    }
  } else {
    try {
      const intervalMs = minutesInt * 60 * 1000;
      const now = Date.now();
      const bucket = Math.floor(now / intervalMs) * intervalMs;
      const result = await pool.query(
        `
        SELECT pct_change
        FROM stock_aggregates
        WHERE stock_symbol = $1 AND interval_minutes = $2
        AND timestamp_bucket = $3
        `,
        [symbol, minutesInt, bucket]
      );
      if (result.rows.length > 0) {
        res.json({
          interval_minutes: minutesInt,
          pct_change: result.rows[0].pct_change,
        });
      } else {
        res.status(404).send("No aggregation data found");
      }
    } catch (e) {
      console.error("Error fetching minute change:", e.message);
      res.status(500).send("Internal server error");
    }
  }
});

// Get all minute changes
app.get("/stock/:symbol/minute-changes", async (req, res) => {
  const { symbol } = req.params;
  const intervals = [1, 2, 3, 5, 10, 15, 30, 60, 120];
  const results = {};

  const subCheck = await pool.query(
    "SELECT 1 FROM stock_subscriptions WHERE stock_symbol = $1",
    [symbol]
  );
  if (subCheck.rowCount === 0) {
    return res.status(404).send(`Stock ${symbol} is not subscribed`);
  }

  for (const interval of intervals.filter((i) => i <= 5)) {
    results[interval] = computePctChange(symbol, interval) || "N/A";
  }

  try {
    const now = Date.now();
    const buckets = intervals
      .filter((i) => i > 5)
      .map((i) => ({
        interval: i,
        bucket: Math.floor(now / (i * 60 * 1000)) * (i * 60 * 1000),
      }));
    const result = await pool.query(
      `
      SELECT interval_minutes, pct_change
      FROM stock_aggregates
      WHERE stock_symbol = $1
      AND (interval_minutes, timestamp_bucket) IN (
        SELECT UNNEST($2::int[]), UNNEST($3::bigint[])
      )
      `,
      [symbol, buckets.map((b) => b.interval), buckets.map((b) => b.bucket)]
    );
    result.rows.forEach((row) => {
      results[row.interval_minutes] = row.pct_change;
    });
    res.json(results);
  } catch (e) {
    console.error("Error fetching minute changes:", e.message);
    res.status(500).send("Internal server error");
  }
});

// Updated Route: Top 20 Subscribed Stocks with Mixed Memory/DB Changes
app.get("/stocks/top", async (req, res) => {
  const { sort } = req.query;
  const sortInterval = sort ? parseInt(sort, 10) : DEFAULT_SORT_MINUTE; // Default sort by 10-minute change
  const limit = DEFAULT_TOP_STOCKS_LIMIT;
  const memoryIntervals = [1, 2, 3, 5];
  const dbIntervals = [10, 15, 30];

  try {
    // Get all subscribed stocks
    const subResult = await pool.query(
      `
      SELECT stock_symbol
      FROM stock_subscriptions
      `
    );
    const subscribedStocks = subResult.rows.map((row) => row.stock_symbol);

    if (subscribedStocks.length === 0) {
      return res.json([]);
    }

    // Get latest cmp from stocks table
    const cmpResult = await pool.query(
      `
      SELECT stock_symbol, cmp
      FROM stocks
      WHERE stock_symbol = ANY($1::varchar[])
      AND (stock_symbol, timestamp) IN (
        SELECT stock_symbol, MAX(timestamp)
        FROM stocks
        WHERE stock_symbol = ANY($1::varchar[])
        GROUP BY stock_symbol
      )
      `,
      [subscribedStocks]
    );

    // Get aggregated changes from DB for intervals > 5 minutes
    const aggResult = await pool.query(
      `
      SELECT stock_symbol, interval_minutes, pct_change
      FROM stock_aggregates
      WHERE stock_symbol = ANY($1::varchar[])
      AND interval_minutes = ANY($2::int[])
      AND (stock_symbol, interval_minutes, timestamp_bucket) IN (
        SELECT stock_symbol, interval_minutes, MAX(timestamp_bucket)
        FROM stock_aggregates
        WHERE stock_symbol = ANY($1::varchar[])
        AND interval_minutes = ANY($2::int[])
        GROUP BY stock_symbol, interval_minutes
      )
      `,
      [subscribedStocks, dbIntervals]
    );

    // Build stock data with memory and DB changes
    const stockMap = new Map();
    cmpResult.rows.forEach((row) => {
      stockMap.set(row.stock_symbol, {
        stock_symbol: row.stock_symbol,
        cmp: row.cmp || "N/A",
      });
    });

    // Add memory-based changes (1, 2, 3, 5 minutes)
    subscribedStocks.forEach((symbol) => {
      if (!stockMap.has(symbol)) {
        stockMap.set(symbol, { stock_symbol: symbol, cmp: "N/A" });
      }
      const stock = stockMap.get(symbol);
      memoryIntervals.forEach((interval) => {
        stock[`${interval}_minute_change`] =
          computePctChange(symbol, interval) || "N/A";
      });
    });

    // Add DB-based changes (10, 15, 30, 60, 120 minutes)
    aggResult.rows.forEach((row) => {
      if (stockMap.has(row.stock_symbol)) {
        const stock = stockMap.get(row.stock_symbol);
        stock[`${row.interval_minutes}_minute_change`] = row.pct_change;
      }
    });

    // Convert to array and sort
    const topStocks = Array.from(stockMap.values())
      .sort((a, b) => {
        const aChange = a[`${sortInterval}_minute_change`] || "N/A";
        const bChange = b[`${sortInterval}_minute_change`] || "N/A";
        if (aChange === "N/A" && bChange === "N/A")
          return a.stock_symbol.localeCompare(b.stock_symbol);
        if (aChange === "N/A") return 1;
        if (bChange === "N/A") return -1;
        return bChange - aChange; // Descending order
      })
      .slice(0, limit);

    res.json(topStocks);
  } catch (e) {
    console.error("Error fetching top stocks:", e.message);
    res.status(500).send("Internal server error");
  }
});

// Add single or multiple stock subscriptions
app.post("/stocks/add", async (req, res) => {
  const stockSymbols = Array.isArray(req.query.stock_symbol)
    ? req.query.stock_symbol
    : [req.query.stock_symbol].filter(Boolean);

  if (!stockSymbols.length) {
    return res.status(400).send("Missing stock_symbol");
  }

  try {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const addedStocks = [];
      for (const stock_symbol of stockSymbols) {
        const result = await client.query(
          `
          INSERT INTO stock_subscriptions (stock_symbol, subscribed_at)
          VALUES ($1, $2)
          ON CONFLICT (stock_symbol) DO NOTHING
          RETURNING *
          `,
          [stock_symbol, Date.now()]
        );
        if (result.rowCount > 0) {
          addedStocks.push(stock_symbol);
        }
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
        // simulateWebSocketData(10, stock_symbol, 1)
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
});

// Remove a stock subscription
app.post("/stocks/remove", async (req, res) => {
  const { stock_symbol } = req.query;
  if (!stock_symbol) {
    return res.status(400).send("Missing stock_symbol");
  }

  try {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // Delete from stock_subscriptions
      const subResult = await client.query(
        `
        DELETE FROM stock_subscriptions
        WHERE stock_symbol = $1
        RETURNING *
        `,
        [stock_symbol]
      );

      if (subResult.rowCount === 0) {
        await client.query("ROLLBACK");
        return res.status(404).send(`Stock ${stock_symbol} not subscribed`);
      }

      // Delete from stock_aggregates
      const aggResult = await client.query(
        `
        DELETE FROM stock_aggregates
        WHERE stock_symbol = $1
        RETURNING *
        `,
        [stock_symbol]
      );

      // Delete from stocks
      const stocksResult = await client.query(
        `
        DELETE FROM stocks
        WHERE stock_symbol = $1
        RETURNING *
        `,
        [stock_symbol]
      );

      await client.query("COMMIT");

      // Stop simulation and clear from memory
      upstoxWebsocket.unsubscribe(stock_symbol);
      // stopSimulation(stock_symbol);
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
});

app.get("/ping", (req, res) => res.send("ok"));

startServer().then(() => {
  app.listen(port, () =>
    console.log(`Server running at http://localhost:${port}`)
  );
});
