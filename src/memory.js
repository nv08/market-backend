"use strict";

const { MEMORY_MAX_WINDOW } = require("./constants");
const { createThrottledLog } = require("./helper");

let currentBuffer = new Map();
let flushingBuffer = new Map();

const throttledLoggers = new Map();

function getThrottledLogger(stock_symbol) {
  if (!throttledLoggers.has(stock_symbol)) {
    throttledLoggers.set(stock_symbol, createThrottledLog(5000));
  }
  return throttledLoggers.get(stock_symbol);
}

function addStockData(data) {
  const { stock_symbol, cmp, open, high, low, close, timestamp } = data;
  const ts = Number(timestamp);

  if (!currentBuffer.has(stock_symbol)) {
    currentBuffer.set(stock_symbol, { dataPoints: [], latestClose: null });
  }

  const stockData = currentBuffer.get(stock_symbol);
  const { dataPoints } = stockData;

  // Update latest close (assuming close is the current value; adjust to cmp if needed)
  if (close !== undefined) {
    stockData.latestClose = close;
  }

  // Store the data point
  if (
    open !== undefined &&
    high !== undefined &&
    low !== undefined &&
    close !== undefined &&
    cmp !== undefined
  ) {
    const existingIndex = dataPoints.findIndex((d) => d.timestamp === ts);
    if (existingIndex !== -1) {
      // Update existing data point
      dataPoints[existingIndex] = {
        timestamp: ts,
        cmp,
        open,
        high,
        low,
        close,
      };
    } else {
      // Add new data point
      dataPoints.push({ timestamp: ts, cmp, open, high, low, close });
    }

    // Sort by timestamp ascending
    dataPoints.sort((a, b) => a.timestamp - b.timestamp);

    // Trim old data points based on MEMORY_MAX_WINDOW
    const now = Date.now();
    while (
      dataPoints.length > 0 &&
      now - dataPoints[0].timestamp > MEMORY_MAX_WINDOW
    ) {
      dataPoints.shift();
    }
  }

  const log = getThrottledLogger(stock_symbol);
  log(
    `Added data for ${stock_symbol}, buffer size: ${dataPoints.length}, ` +
      `timestamps: ${dataPoints
        .map((d) => new Date(d.timestamp).toISOString())
        .join(", ")}, ` +
      `latestClose: ${stockData.latestClose}`
  );
}

function removeStockFromMemory(stock_symbol) {
  if (currentBuffer.has(stock_symbol)) {
    currentBuffer.delete(stock_symbol);
    throttledLoggers.delete(stock_symbol);
    console.log(`Removed ${stock_symbol} from memory buffer`);
  }
}

function computePctChange(stock_symbol, intervalMinutes) {
  const stockData = currentBuffer.get(stock_symbol) || {
    dataPoints: [],
    latestClose: null,
  };
  const { dataPoints, latestClose } = stockData;

  console.log(
    `Computing ${intervalMinutes}-minute change for ${stock_symbol}:`
  );
  console.log(`  Now: ${new Date().toISOString()}`);
  console.log(
    `  Buffer size: ${dataPoints.length}, timestamps: ${dataPoints
      .map((d) => new Date(d.timestamp).toISOString())
      .join(", ")}`
  );
  console.log(`  Latest Close: ${latestClose}`);

  if (dataPoints.length === 0) {
    console.log(`  No data points in buffer`);
    return 0;
  }

  // Sort dataPoints ascending (should already be sorted, but ensure)
  dataPoints.sort((a, b) => a.timestamp - b.timestamp);

  // Round current time to the nearest minute and get the last completed minute
  const now = Date.now();
  const roundToMinute = (ts) => {
    const date = new Date(ts);
    date.setSeconds(0);
    date.setMilliseconds(0);
    return date.getTime();
  };
  const currentMinute = roundToMinute(now);
  const lastCompletedMinute = currentMinute - 60 * 1000; // Previous minute

  // Filter dataPoints up to the last completed minute for the interval
  const cutoff = lastCompletedMinute - (intervalMinutes - 1) * 60 * 1000;
  const filtered = dataPoints.filter(
    (d) => d.timestamp >= cutoff && d.timestamp <= lastCompletedMinute
  );

  console.log(
    `  Filtered size: ${filtered.length}, timestamps: ${filtered
      .map((d) => new Date(d.timestamp).toISOString())
      .join(", ")}`
  );

  if (filtered.length < intervalMinutes) {
    console.log(
      `  Not enough data points for ${intervalMinutes}-minute interval`
    );
    return intervalMinutes === 1 && filtered.length === 0 ? 0 : "N/A";
  }

  // For the specified interval, use the earliest open and latest close within filtered
  const firstPrice = filtered[0].open; // Start of the interval
  const lastPrice = filtered[filtered.length - 1].close; // End of the interval (last completed minute)
  const pctChange = Number(
    (((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2)
  );

  console.log(
    `  Computed ${intervalMinutes}-minute change: ${pctChange}% (first=${firstPrice} (open), last=${lastPrice} (close))`
  );
  return pctChange;
}

module.exports = {
  addStockData,
  computePctChange,
  removeStockFromMemory,
  currentBuffer,
  flushingBuffer,
};
