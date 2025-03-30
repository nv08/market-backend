"use strict";

const { MEMORY_MAX_WINDOW, THROTTLED_LOG_TIMER } = require("./constants");
const { throttledCallback } = require("./helper");

let currentBuffer = new Map();
let flushingBuffer = new Map();

const throttledLoggers = new Map();

function getThrottledLogger(stock_symbol) {
  if (!throttledLoggers.has(stock_symbol)) {
    throttledLoggers.set(stock_symbol, throttledCallback(THROTTLED_LOG_TIMER));
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
  log(() => {
    console.table(dataPoints.map(d => ({
      Time: new Date(d.timestamp).toLocaleTimeString(),
      Symbol: stock_symbol,
      CMP: d.cmp,
      Open: d.open,
      High: d.high,
      Low: d.low,
      Close: d.close
    })).slice(-10));
  });
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
  const { dataPoints } = stockData;

  if (dataPoints.length === 0) {
    console.log(`No data points in buffer for ${stock_symbol}`);
    return "N/A";
  }

  // Sort data points by timestamp (ascending)
  dataPoints.sort((a, b) => a.timestamp - b.timestamp);

  const now = Date.now();
  const roundToMinute = (ts) => {
    const date = new Date(ts);
    date.setSeconds(0);
    date.setMilliseconds(0);
    return date.getTime();
  };

  // Define minute boundaries
  const currentMinuteStart = roundToMinute(now);           // e.g., 9:15:00
  const previousMinuteStart = currentMinuteStart - 60 * 1000; // e.g., 9:14:00
  const secondLastMinuteStart = previousMinuteStart - 60 * 1000; // e.g., 9:13:00

  if (intervalMinutes === 0) {
    // Case: Nearest minute open to latest close
    // Filter data points in the current minute
    const currentMinuteData = dataPoints.filter(
      (d) => d.timestamp >= currentMinuteStart && d.timestamp <= now
    );

    if (currentMinuteData.length === 0) {
      console.log(`No data in current minute for ${stock_symbol}`);
      return "N/A";
    }

    const firstPrice = currentMinuteData[0].open;           // Open of nearest minute
    const lastPrice = dataPoints[dataPoints.length - 1].close; // Latest close overall
    const pctChange = Number(
      (((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2)
    );

    console.log(
      `Computed 0-minute change for ${stock_symbol}: ${pctChange}% ` +
      `(open of nearest minute=${firstPrice}, latest close=${lastPrice})`
    );
    return pctChange;
  } else if (intervalMinutes === 1) {
    // Case: Open of second last minute to close of previous minute
    // Filter data for the second last minute
    const secondLastMinuteData = dataPoints.filter(
      (d) =>
        d.timestamp >= secondLastMinuteStart &&
        d.timestamp < previousMinuteStart
    );
    // Filter data for the previous minute
    const previousMinuteData = dataPoints.filter(
      (d) =>
        d.timestamp >= previousMinuteStart &&
        d.timestamp < currentMinuteStart
    );

    if (secondLastMinuteData.length === 0 || previousMinuteData.length === 0) {
      console.log(
        `Insufficient data for 1-minute interval for ${stock_symbol}`
      );
      return "N/A";
    }

    const firstPrice = secondLastMinuteData[0].open;           // Open of second last minute
    const lastPrice = previousMinuteData[previousMinuteData.length - 1].close; // Close of previous minute
    const pctChange = Number(
      (((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2)
    );

    console.log(
      `Computed 1-minute change for ${stock_symbol}: ${pctChange}% ` +
      `(open of 2nd last minute=${firstPrice}, close of prev minute=${lastPrice})`
    );
    return pctChange;
  } else {
    // For other intervals (not specified, so generalize)
    const intervalStart = currentMinuteStart - intervalMinutes * 60 * 1000;
    const lastCompletedMinute = currentMinuteStart - 60 * 1000;

    const filtered = dataPoints.filter(
      (d) => d.timestamp >= intervalStart && d.timestamp <= lastCompletedMinute
    );

    if (filtered.length === 0) {
      console.log(`No data points in ${intervalMinutes}-minute interval`);
      return "N/A";
    }

    const firstPrice = filtered[0].open;
    const lastPrice = filtered[filtered.length - 1].close;
    const pctChange = Number(
      (((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2)
    );

    console.log(
      `Computed ${intervalMinutes}-minute change: ${pctChange}% ` +
      `(first=${firstPrice}, last=${lastPrice})`
    );
    return pctChange;
  }
}

module.exports = {
  addStockData,
  computePctChange,
  removeStockFromMemory,
  currentBuffer,
  flushingBuffer,
};
