const { MarketDataApiToOHLC, MarketDataWebsocketToOHLC, colorize } = require("../helper");
const { Buffer } = require("buffer");
const { FeedResponse } = require("./marketDataFeed_pb");
const WebSocket = require("ws");
const UpstoxClient = require("upstox-js-sdk");

const apiVersion = "2.0";
let defaultClient = UpstoxClient.ApiClient.instance;
let OAUTH2 = defaultClient.authentications["OAUTH2"];

const getStockData = async (instrument) => {
  const url = `https://api.upstox.com/v2/historical-candle/intraday/NSE_EQ|${instrument}/1minute`;
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const data = await response.json();
    if (data && data.status === "success") {
      const modifiedData = MarketDataApiToOHLC(data.data, instrument);
      return modifiedData;
    }
    return data;
  } catch (error) {
    console.error(colorize.error(`Error fetching stock data: ${error}`));
    return {};
  }
};

class UpstoxWebSocket {
  constructor(token, onOpen, onClose, onError) {
    OAUTH2.accessToken = token;
    this.token = token;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.onError = onError;
    this.ws = null;
    this.subscriptions = [];
    this.callbacks = new Map();
    this.connectWebSocket();
  }

  async getUrl() {
    return new Promise((resolve, reject) => {
      let apiInstance = new UpstoxClient.WebsocketApi();
      apiInstance.getMarketDataFeedAuthorize(apiVersion, (error, data) => {
        if (error) {
          console.error(colorize.error(`Error fetching WebSocket URL: ${error.status}`));
          reject(error);
        } else {
          resolve(data.data.authorizedRedirectUri);
        }
      });
    });
  }

  decodeProtobuf(buffer) {
    try {
      return FeedResponse.deserializeBinary(buffer);
    } catch (error) {
      console.log(colorize.warning(`Failed to decode protobuf: ${error}`));
      return null;
    }
  }

  async connectWebSocket() {
    try {
      const wsUrl = await this.getUrl();
      this.ws = new WebSocket(wsUrl, {
        headers: {
          "Api-Version": apiVersion,
          Authorization: `Bearer ${this.token}`,
        },
        followRedirects: true,
      });

      this.ws.on("open", () => {
        this.onOpen();
        console.log(colorize.success("Connected"));
        this.subscriptions.forEach(({ data, callback }) => {
          this.sendSubscription(data, callback);
        });
        this.subscriptions = [];
      });

      this.ws.on("close", () => {
        this.onClose();
        console.log(colorize.warning("Disconnected"));
      });

      this.ws.on("error", (error) => {
        this.onError(error);
        console.error(colorize.error("WebSocket error:", error.status));
      });

      this.ws.on("message", (data) => {
        const buffer = Buffer.from(data);
        const response = this.decodeProtobuf(buffer);
        if (response) {
          const responseObj = response.toObject();
          // console.log(
          //   "Raw WebSocket response:",
          //   JSON.stringify(responseObj, null, 2)
          // );

          const modifiedResponses = MarketDataWebsocketToOHLC(responseObj);
          // console.log("Modified responses array:", modifiedResponses);

          // Iterate over all OHLC objects and invoke callbacks
          modifiedResponses.forEach((response) => {
            const sym = response?.stock_symbol;
            if (sym) {
              const cb = this.callbacks.get(sym);
              if (cb) {
                cb(response);
              } else {
                console.log(colorize.warning(`No callback found for symbol ${sym}`));
              }
            }
          });
        } else {
          console.warn("Received invalid Protobuf response");
        }
      });
    } catch (error) {
      console.log(colorize.error(`WebSocket connection error: ${error.status} ${error.message}`));
      this.onError(error);
    }
  }

  sendSubscription(data, callback) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn(colorize.warning("WebSocket not open, cannot send subscription"));
      return;
    }
    this.ws.send(Buffer.from(JSON.stringify(data)));
    console.log(colorize.info(`Subscription sent: ${JSON.stringify(data)}`));
    const [_, symbol] = data.data.instrumentKeys[0].split("|");
    this.callbacks.set(symbol, callback);
  }

  subscribe(symbol, callback) {
    const data = {
      guid: `guid-${Date.now()}`,
      method: "sub",
      data: {
        mode: "full",
        instrumentKeys: [`NSE_EQ|${symbol}`],
      },
    };

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendSubscription(data, callback);
    } else {
      this.subscriptions.push({ data, callback });
      console.log(
        colorize.info(
        "WebSocket is not open. Subscription added to pending list.")
      );
    }
  }

  unsubscribe(symbol) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn(colorize.warning("WebSocket not open, cannot unsubscribe"));
      return;
    }

    const data = {
      guid: `guid-${Date.now()}`,
      method: "unsub",
      data: {
        mode: "full",
        instrumentKeys: [`NSE_EQ|${symbol}`],
      },
    };

    this.ws.send(Buffer.from(JSON.stringify(data)));
    console.log(colorize.info(`Unsubscription sent: ${JSON.stringify(data)}`));
    this.callbacks.delete(`NSE_EQ|${symbol}`);
  }

  close() {
    if (this.ws) {
      this.ws.close();
      console.log(colorize.warning("WebSocket connection closed manually"));
    }
  }
}

const token = process.env.UPSTOX_TOKEN;

const onOpen = () => console.log(colorize.success("WebSocket opened"));
const onClose = () => console.log(colorize.warning("WebSocket closed"));
const onError = () => console.error(colorize.error("WebSocket error"));

const upstoxWebsocket = new UpstoxWebSocket(token, onOpen, onClose, onError);

module.exports = {
  upstoxWebsocket,
  getStockData,
};
