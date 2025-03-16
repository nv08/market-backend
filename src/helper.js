const _ = require("lodash");

// Factory function to create a throttled log function with a specified interval
const createThrottledLog = (interval) => {
  return _.throttle((message) => {
    console.log(message);
  }, interval);
};

const MarketDataApiToOHLC = (data, symbol) => {
  if (!data || !data.candles || data.candles.length === 0) {
    console.log("stock data returned empty");
    return [];
  }
  const modifiedData = data.candles.map((candle) => ({
    timestamp: new Date(candle[0]).getTime(),
    open: candle[1],
    high: candle[2],
    low: candle[3],
    close: candle[4],
    stock_symbol: symbol,
    cmp: candle[4],
  }));
  return modifiedData;
};

const MarketDataWebsocketToOHLC = (data) => {
  if (!data || !data.feedsMap) return [];

  const transformedData = data.feedsMap.map(([key, feed]) => {
    console.log(key, "is this the instrument");
    const ohlc = feed.ff.marketff.marketohlc.ohlcList.find(
      (entry) => entry.interval === "I1"
    );

    return {
      timestamp: ohlc ? new Date(parseInt(ohlc.ts)).getTime() : null,
      open: ohlc ? ohlc.open : null,
      high: ohlc ? ohlc.high : null,
      low: ohlc ? ohlc.low : null,
      close: ohlc ? ohlc.close : null,
      stock_symbol: key,
      cmp: ohlc ? ohlc.close : null,
    };
  });

  return transformedData;
};

module.exports = {
  createThrottledLog,
  MarketDataApiToOHLC,
  MarketDataWebsocketToOHLC,
};
