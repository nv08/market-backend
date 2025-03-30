const _ = require("lodash");

const throttledCallback = (interval) => {
  return _.throttle((cb) => {
    cb();
  }, interval);
};

const MarketDataApiToOHLC = (data, symbol) => {
  if (!data || !data.candles || data.candles.length === 0) {
    console.log(colorize.warning("stock data returned empty"));
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

function MarketDataWebsocketToOHLC(data) {
  if (!data || !data.feedsMap || !Array.isArray(data.feedsMap)) {
    console.warn("Invalid feedsMap data:", data);
    return [];
  }

  const ohlcDataArray = data.feedsMap
    .map(([feedKey, feedData]) => {
      const stock_symbol = feedKey.split("|")[1];
      const { marketohlc, ltpc } = feedData.ff.marketff || {};

      if (!ltpc || !ltpc.ltp) {
        console.warn(`Missing LTP data for ${stock_symbol}`);
        return null;
      }

      // Round timestamp to nearest minute
      const roundToNearestMinute = (ts) => {
        const date = new Date(ts);
        const seconds = date.getSeconds();
        const milliseconds = date.getMilliseconds();
        // Round based on seconds: < 30s → floor, >= 30s → ceil
        if (seconds >= 30) {
          date.setMinutes(date.getMinutes() + 1);
        }
        date.setSeconds(0);
        date.setMilliseconds(0);
        return date.getTime();
      };

      let timestamp = ltpc.ltt
        ? roundToNearestMinute(ltpc.ltt)
        : roundToNearestMinute(Date.now());
      let ohlcData = {
        timestamp,
        stock_symbol,
        cmp: ltpc.ltp,
        close: ltpc.ltp,
        open: ltpc.ltp,
        high: ltpc.ltp,
        low: ltpc.ltp,
      };

      if (marketohlc && marketohlc.ohlcList) {
        // Find the latest I1 candle by timestamp
        const oneMinCandles = marketohlc.ohlcList.filter(
          (c) => c.interval === "I1"
        );
        const latestCandle =
          oneMinCandles.length > 0
            ? oneMinCandles.reduce((latest, current) =>
                current.ts > latest.ts ? current : latest
              )
            : null;

        if (latestCandle) {
          timestamp = roundToNearestMinute(latestCandle.ts); // Use candle ts, rounded
          ohlcData = {
            timestamp,
            stock_symbol,
            cmp: ltpc.ltp, // Real-time LTP
            open: latestCandle.open,
            high: latestCandle.high,
            low: latestCandle.low,
            close: latestCandle.close,
          };
        } else {
          console.warn(
            `No 1-minute candle found for ${stock_symbol}, using LTP as fallback`
          );
        }
      } else {
        console.warn(`No OHLC list for ${stock_symbol}, using LTP as fallback`);
      }

      return ohlcData;
    })
    .filter(Boolean);
  // console.log("Processed WebSocket feedsMap into OHLC array:", ohlcDataArray);

  return ohlcDataArray;
}

const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m"
};

const styles = {
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  italic: "\x1b[3m",
  underline: "\x1b[4m",
  inverse: "\x1b[7m",
  hidden: "\x1b[8m",
  strike: "\x1b[9m"
};

class ColorString {
  constructor(str) {
    this.str = str;
    this.modifications = [];
  }

  bold() {
    this.modifications.push(styles.bold);
    return this.toString();
  }

  italic() {
    this.modifications.push(styles.italic);
    return this.toString();
  }

  underline() {
    this.modifications.push(styles.underline);
    return this.toString();
  }

  toString() {
    const allModifications = this.modifications.join('');
    const colorCode = this.str.slice(0, 5);
    return `${colorCode}═══> ${allModifications}${this.str.slice(5)}${colors.reset}`;
  }
}

const colorize = {
  error: (text) => new ColorString(`${colors.red}${text}`).bold(),
  success: (text) => new ColorString(`${colors.green}${text}`).bold(),
  warning: (text) => new ColorString(`${colors.yellow}${text}`).bold(),
  info: (text) => new ColorString(`${colors.blue}${text}`).bold(),
  debug: (text) => new ColorString(`${colors.magenta}${text}`).bold(),
  system: (text) => new ColorString(`${colors.cyan}${text}`).bold(),
};


module.exports = {
  throttledCallback,
  MarketDataApiToOHLC,
  MarketDataWebsocketToOHLC,
  colorize
};
