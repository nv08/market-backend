"use strict";

const { MEMORY_MAX_WINDOW } = require("./constants");
const { createThrottledLog } = require("./helper");

let currentBuffer = new Map(); // { stock_symbol: [{ timestamp, cmp, ... }, ...] }
let flushingBuffer = new Map();

// Map to store throttled loggers per stock symbol
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
    currentBuffer.set(stock_symbol, []);
  }

  const stockData = currentBuffer.get(stock_symbol);
  const existing = stockData.find((d) => d.timestamp === ts);
  if (existing) {
    Object.assign(existing, { cmp, open, high, low, close, timestamp: ts });
  } else {
    stockData.push({ timestamp: ts, cmp, open, high, low, close });
  }

  while (
    stockData.length > 0 &&
    ts - stockData[0].timestamp > MEMORY_MAX_WINDOW
  ) {
    stockData.shift();
  }

  const log = getThrottledLogger(stock_symbol);
  log(`Added data for ${stock_symbol}, buffer size: ${stockData.length}`);
}

// New function to remove stock from memory
function removeStockFromMemory(stock_symbol) {
  if (currentBuffer.has(stock_symbol)) {
    currentBuffer.delete(stock_symbol);
    throttledLoggers.delete(stock_symbol); // Clean up logger too
    console.log(`Removed ${stock_symbol} from memory buffer`);
  }
}

function computePctChange(stock_symbol, intervalMinutes) {
  const stockData = currentBuffer.get(stock_symbol) || [];
  const now = Date.now();
  const cutoff = now - intervalMinutes * 60 * 1000;

  const filtered = stockData.filter((d) => d.timestamp >= cutoff);
  if (filtered.length < 2) return null;

  const firstPrice = filtered[0].close;
  const lastPrice = filtered[filtered.length - 1].close;
  return Number((((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2));
}

function getLatestStock(stock_symbol) {
  const stockData = currentBuffer.get(stock_symbol);
  if (!stockData || stockData.length === 0) return null;
  return stockData[stockData.length - 1];
}

module.exports = {
  addStockData,
  computePctChange,
  getLatestStock,
  removeStockFromMemory, // Export new function
  currentBuffer,
  flushingBuffer,
};
