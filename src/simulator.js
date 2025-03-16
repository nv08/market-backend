"use strict";

const { addStockData } = require("./memory");

const generateRandomStockData = (
  stock_symbol,
  basePrice,
  elapsedMinutes,
  pctIncreasePerMinute
) => {
  const priceIncrease = (pctIncreasePerMinute / 100) * elapsedMinutes;
  const calculatedPrice = basePrice * (1 + priceIncrease);
  return {
    stock_symbol: stock_symbol,
    cmp: calculatedPrice.toFixed(2),
    open: basePrice.toFixed(2),
    high: (calculatedPrice + 1).toFixed(2),
    low: (calculatedPrice - 0.5).toFixed(2),
    close: calculatedPrice.toFixed(2),
    timestamp: new Date().getTime(),
  };
};

const activeSimulations = new Map();

const simulateWebSocketData = (rps, stock_symbol, pctIncreasePerMinute = 1) => {
  const interval = 1000 / rps;
  const basePrice = 150.0;
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
