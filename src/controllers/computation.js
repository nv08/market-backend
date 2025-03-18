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

  try {
    const subResult = await pool.query(
      `SELECT stock_symbol FROM stock_subscriptions`
    );
    const subscribedStocks = subResult.rows.map((row) => row.stock_symbol);

    if (subscribedStocks.length === 0) return res.json([]);

    const stockMap = new Map();
    subscribedStocks.forEach((symbol) => {
      const stockData = currentBuffer.get(symbol) || { latestClose: "N/A" };
      stockMap.set(symbol, {
        stock_symbol: symbol,
        cmp: stockData.latestClose !== null ? stockData.latestClose : "N/A", // Real-time from memory
      });
    });

    subscribedStocks.forEach((symbol) => {
      if (!stockMap.has(symbol))
        stockMap.set(symbol, { stock_symbol: symbol, cmp: "N/A" });
      const stock = stockMap.get(symbol);
      memoryIntervals.forEach((interval) => {
        const pctChange = computePctChange(symbol, interval);
        stock[`${interval}_minute_change`] =
          pctChange === null ? "N/A" : pctChange;
      });
    });

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
  } catch (e) {
    console.error("Error fetching top stocks:", e.message);
    res.status(500).send("Internal server error");
  }
};

module.exports = {
  getTopStocks,
};
