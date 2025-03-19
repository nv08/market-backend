const { pool } = require("../db");
const { computePctChange, currentBuffer } = require("../memory");
const {
  DEFAULT_TOP_STOCKS_LIMIT,
  DEFAULT_SORT_MINUTE,
} = require("../constants");

const getTopStocks = async (req, res) => {
  const { sort } = req.query;
  const sortInterval = sort ? parseInt(sort, 10) : DEFAULT_SORT_MINUTE;
  const limit = DEFAULT_TOP_STOCKS_LIMIT;
  const memoryIntervals = [1, 2, 3, 4, 5];
  const dbIntervals = [10, 15, 30, 60];

  try {
    // Get subscribed stocks
    const subResult = await pool.query(
      `SELECT stock_symbol FROM stock_subscriptions`
    );
    const subscribedStocks = subResult.rows.map((row) => row.stock_symbol);

    if (subscribedStocks.length === 0) return res.json([]);

    const client = await pool.connect();
    try {
      // Fetch latest current market price (cmp)
      const latestPricesResult = await client.query(
        `
        SELECT DISTINCT ON (stock_symbol) stock_symbol, cmp
        FROM stocks
        WHERE stock_symbol = ANY($1::text[])
        ORDER BY stock_symbol, timestamp DESC
        `,
        [subscribedStocks]
      );

      const stockMap = new Map();
      latestPricesResult.rows.forEach((row) => {
        stockMap.set(row.stock_symbol, {
          stock_symbol: row.stock_symbol,
          cmp: row.cmp !== null ? row.cmp : "N/A",
        });
      });

      // Fetch precomputed changes for DB intervals (10, 15, 30, 60)
      const aggregatesResult = await client.query(
        `
        SELECT stock_symbol, interval_minutes, pct_change
        FROM stock_aggregates
        WHERE stock_symbol = ANY($1::text[])
        AND interval_minutes = ANY($2::integer[])
        AND timestamp_bucket = (
          SELECT MAX(timestamp_bucket)
          FROM stock_aggregates
          WHERE stock_symbol = stock_aggregates.stock_symbol
          AND interval_minutes = stock_aggregates.interval_minutes
        )
        `,
        [subscribedStocks, dbIntervals]
      );

      aggregatesResult.rows.forEach((row) => {
        const stock = stockMap.get(row.stock_symbol) || {
          stock_symbol: row.stock_symbol,
          cmp: "N/A",
        };
        stock[`${row.interval_minutes}_minute_change`] =
          row.pct_change !== null ? row.pct_change : "N/A";
        stockMap.set(row.stock_symbol, stock);
      });

      // Compute percentage changes for memory intervals (1, 2, 3, 4, 5)
      subscribedStocks.forEach((symbol) => {
        const stock = stockMap.get(symbol) || {
          stock_symbol: symbol,
          cmp: "N/A",
        };
        memoryIntervals.forEach((interval) => {
          const pctChange = computePctChange(symbol, interval);
          stock[`${interval}_minute_change`] =
            pctChange === null ? "N/A" : pctChange;
        });
        stockMap.set(symbol, stock);
      });

      // Sort and limit the results
      const topStocks = Array.from(stockMap.values())
        .sort((a, b) => {
          const aChange = a[`${sortInterval}_minute_change`] || "N/A";
          const bChange = b[`${sortInterval}_minute_change`] || "N/A";
          if (aChange === "N/A" && bChange === "N/A")
            return a.stock_symbol.localeCompare(b.stock_symbol);
          if (aChange === "N/A") return 1;
          if (bChange === "N/A") return -1;
          return bChange - aChange;
        })
        .slice(0, limit);

      res.json(topStocks);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("Error fetching top stocks:", e.message);
    res.status(500).send("Internal server error");
  }
};

module.exports = {
  getTopStocks,
};
