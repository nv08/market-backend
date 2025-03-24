"use strict";

const express = require("express");
const { setupDatabase, pool } = require("./db");
const { flushToDatabase, scheduleAggregations } = require("./aggregator");
const cors = require("cors");
require("dotenv").config();
const { FLUSHING_TO_DB_INTERVAL } = require("./constants");
const { addStock, removeStock } = require("./controllers/crud");
const { getTopStocks } = require("./controllers/computation");
const { addStockData } = require("./memory");
const { fetchAndStoreInitialData } = require("./query");
const { upstoxWebsocket } = require("./websocket");
const { colorize } = require("./helper");
const { simulateWebSocketData } = require("./simulator");
const {notification} = require("./controllers/notifications");

const app = express();
const port = process.env.PORT || 3000;
app.use(cors());

console.log(colorize.red("Starting server...").bold());

function handleWebSocketData(data) {
  if (data) {
    addStockData(data);
  }
}

async function startServer() {
  await setupDatabase();
  setInterval(flushToDatabase, FLUSHING_TO_DB_INTERVAL);
  scheduleAggregations();
  // setInterval(runAggregations, RUNNING_AGGREGATIONS_INTERVAL);
  // simulateWebSocketData(1, "INE040H01021", 1)
  // simulateWebSocketData(1, "INE00CE01017", -5)

  try {
    const result = await pool.query(
      `SELECT stock_symbol FROM stock_subscriptions`
    );
    const subscriptions = result.rows;
    if (subscriptions.length === 0) {
      console.log("No stock subscriptions found at startup");
    } else {
      for (const { stock_symbol } of subscriptions) {
        await fetchAndStoreInitialData(stock_symbol);
        upstoxWebsocket.subscribe(stock_symbol, handleWebSocketData);
      }
      console.log(
        `Initialized ${subscriptions.length} stocks with WebSocket subscriptions`
      );
    }
  } catch (e) {
    console.error("Error querying stock subscriptions:", e.message);
  }

  console.log("Server started");
}

app.get("/stocks/top", getTopStocks);
app.post("/stocks/add", addStock);
app.post("/stocks/remove", removeStock);
app.get("/notifications", notification);

app.get("/ping", (req, res) => res.send("ok"));

startServer().then(() => {
  app.listen(port, () =>
    console.log(`Server running at http://localhost:${port}`)
  );
});
