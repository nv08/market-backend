const { MarketDataApiToOHLC, MarketDataWebsocketToOHLC } = require("../helper");
const { Buffer } = require("buffer");
const { FeedResponse } = require("./marketDataFeed_pb");
const WebSocket = require("ws");

const getStockData = async (instrument) => {
  const url = `https://api.upstox.com/v2/historical-candle/intraday/NSE_EQ|${instrument}/1minute`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    if (data && data.status === "success") {
      const modifiedData = MarketDataApiToOHLC(data, instrument);
      return modifiedData;
    }
    return data;
  } catch (error) {
    console.error("Error fetching stock data:", error);
    return {};
  }
};

class UpstoxWebSocket {
  constructor(token, onOpen, onClose, onError) {
    this.token = token;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.onError = onError;
    this.ws = null;
    this.subscriptions = [];
    this.connectWebSocket();
  }

  async getUrl() {
    const apiUrl = "https://api-v2.upstox.com/feed/market-data-feed/authorize";
    let headers = {
      "Content-type": "application/json",
      Authorization: "Bearer " + this.token,
    };
    try {
      const response = await fetch(apiUrl, {
        method: "GET",
        headers: headers,
      });
      if (!response.ok) {
        throw new Error(`Network response was not ok: ${response.status}`);
      }
      const res = await response.json();
      return res.data.authorizedRedirectUri;
    } catch (error) {
      console.error("Error fetching WebSocket URL:", error);
      throw error;
    }
  }

  async blobToArrayBuffer(blob) {
    if ("arrayBuffer" in blob) return await blob.arrayBuffer();
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error);
      reader.readAsArrayBuffer(blob);
    });
  }

  decodeProtobuf(buffer) {
    try {
      return FeedResponse.deserializeBinary(buffer);
    } catch (error) {
      console.warn("Failed to decode protobuf:", error);
      return null;
    }
  }

  async connectWebSocket() {
    try {
      const wsUrl = await this.getUrl();
      this.ws = new WebSocket(wsUrl);

      this.ws.binaryType = "arraybuffer";

      this.ws.onopen = () => {
        this.onOpen();
        console.log("Connected");
        // Send any pending subscriptions
        this.subscriptions.forEach(({ data, callback }) => {
          this.sendSubscription(data, callback);
        });
        this.subscriptions = [];
      };

      this.ws.onclose = () => {
        this.onClose();
        console.log("Disconnected");
      };

      this.ws.onerror = (error) => {
        this.onError(error);
        console.log("WebSocket error:", error);
      };

      this.ws.onmessage = async (event) => {
        const arrayBuffer = event.data;
        let buffer = Buffer.from(arrayBuffer);
        let response = this.decodeProtobuf(buffer);
        if (response) {
          console.log("Received response:", response.toObject());
        }
      };
    } catch (error) {
      console.error("WebSocket connection error:", error);
    }
  }

  sendSubscription(data, callback) {
    this.ws.send(Buffer.from(JSON.stringify(data)));
    this.ws.onmessage = async (event) => {
      const arrayBuffer = event.data;
      let buffer = Buffer.from(arrayBuffer);
      let response = this.decodeProtobuf(buffer);
      callback(response.toObject());
    };
  }

  subscribe(data, callback) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendSubscription(data, callback);
    } else {
      this.subscriptions.push({ data, callback });
      console.warn(
        "WebSocket is not open. Subscription added to pending list."
      );
    }
    console.log(`Subscription sent: ${JSON.stringify(data)}`);
  }

  unsubscribe(symbol) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn("WebSocket is not open, cannot unsubscribe");
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

    const message = Buffer.from(JSON.stringify(data));
    this.ws.send(message);
    console.log(`Unsubscription sent: ${JSON.stringify(data)}`);
  }

  close() {
    if (this.ws) {
      this.ws.close();
      console.log("WebSocket connection closed manually");
    }
  }
}

module.exports = {
  UpstoxWebSocket,
  getStockData,
};
