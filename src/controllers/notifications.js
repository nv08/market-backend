// server/routes/notifications.js
const express = require("express");
const router = express.Router();

// Simulated stock data (replace with your actual data source later)
const stockData = {
  AAPL: { price: 150, lastPrice: 145 },
  TSLA: { price: 700, lastPrice: 680 },
};

const notification = (req, res) => {
  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  // Function to check for notification conditions
  const checkForNotifications = () => {
    Object.entries(stockData).forEach(([symbol, data]) => {
      // Simple condition: price increased by more than 2%
      const pctChange = ((data.price - data.lastPrice) / data.lastPrice) * 100;
      if (pctChange > 2) {
        const eventData = JSON.stringify({
          symbol,
          message: `${symbol} price increased by ${pctChange.toFixed(2)}%!`,
        });
        res.write(`data: ${eventData}\n\n`);
        // Update lastPrice to avoid repeated notifications
        data.lastPrice = data.price;
      }
    });
  };

  // Check every 5 seconds (adjust as needed)
  const intervalId = setInterval(checkForNotifications, 5000);

  // Simulate price changes (for demo purposes)
  const simulatePriceChange = () => {
    stockData.AAPL.price += Math.random() * 5 - 2; // Random change between -2 and +3
    stockData.TSLA.price += Math.random() * 10 - 5;
  };
  const priceIntervalId = setInterval(simulatePriceChange, 10000);

  // Clean up when client disconnects
  req.on("close", () => {
    clearInterval(intervalId);
    clearInterval(priceIntervalId);
    res.end();
  });
};

module.exports = { notification };