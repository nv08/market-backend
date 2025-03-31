"use strict";

const { addStockData } = require("./memory");

const generateRandomStockData = (
  stock_symbol,
  basePrice,
  elapsedMinutes,
  pctIncreasePerMinute
) => {
  // Calculate the compounded price
  const priceIncreasePerMinute = pctIncreasePerMinute / 100;
  // Using compound interest formula: A = P(1 + r)^t
  const compoundedPrice = basePrice * Math.pow(1 + priceIncreasePerMinute, elapsedMinutes);
  
  // Calculate previous close (price at elapsedMinutes - 1)
  const previousElapsedMinutes = Math.max(elapsedMinutes - 1, 0);
  const previousClose = basePrice * Math.pow(1 + priceIncreasePerMinute, previousElapsedMinutes);

  return {
    stock_symbol: stock_symbol,
    cmp: compoundedPrice.toFixed(2),
    open: previousClose.toFixed(2), // Open is the previous close
    high: (compoundedPrice + 1).toFixed(2),
    low: (compoundedPrice - 0.5).toFixed(2),
    close: compoundedPrice.toFixed(2),
    timestamp: new Date().getTime(),
  };
};

const activeSimulations = new Map();

const simulateWebSocketData = (rps, stock_symbol, pctIncreasePerMinute = 1) => {
  const interval = 1000 / rps;
  const basePrice = 100;
  const startTime = Date.now();

  const intervalId = setInterval(() => {
    const now = Date.now();
    const elapsedMinutes = (now - startTime) / 60000;
    const sampleData = generateRandomStockData(
      stock_symbol,
      basePrice,
      elapsedMinutes,
      pctIncreasePerMinute
    );
    // console.log(
    //   `[${stock_symbol}] t=${(elapsedMinutes * 60).toFixed(1)}s, close=${
    //     sampleData.close
    //   }`
    // );
    addStockData(sampleData);
  }, interval);

  activeSimulations.set(stock_symbol, intervalId);
};

const stopSimulation = (stock_symbol) => {
  const intervalId = activeSimulations.get(stock_symbol);
  if (intervalId) {
    clearInterval(intervalId);
    activeSimulations.delete(stock_symbol);
    console.log(`Stopped simulation for ${stock_symbol}`);
  }
};

module.exports = { simulateWebSocketData, stopSimulation };
