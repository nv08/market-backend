const { currentBuffer, computePctChange } = require("../memory");
const {
  NOTIFICATION_THRESHOLDS,
  NOTIFICATION_CHECK_INTERVAL,
  NOTIFICATION_DISPATCH_INTERVAL,
  NOTIFICATION_COOLDOWN,
  MAX_NOTIFICATIONS_PER_BATCH,
} = require("../constants");

// State management
const notificationQueue = [];
const lastNotificationTime = new Map(); // Tracks cooldown per symbol:interval

const isStockInCooldown = (symbol, interval) => {
  const key = `${symbol}:${interval}`;
  const lastTime = lastNotificationTime.get(key);
  return lastTime && Date.now() - lastTime < NOTIFICATION_COOLDOWN;
};

const checkStockThresholds = (symbol, stockData) => {
  if (!stockData.latestClose || stockData.dataPoints.length === 0) {
    return [];
  }

  const notifications = [];

  // Check each interval independently
  for (const [intervalStr, threshold] of Object.entries(
    NOTIFICATION_THRESHOLDS
  )) {
    const interval = parseInt(intervalStr, 10);

    // Skip if stock is in cooldown for this interval
    if (isStockInCooldown(symbol, interval)) {
      continue;
    }

    const pctChange = computePctChange(symbol, interval);
    if (pctChange === "N/A") {
      continue;
    }

    const absPctChange = Math.abs(pctChange);
    if (absPctChange >= threshold) {
      lastNotificationTime.set(`${symbol}:${interval}`, Date.now());
      notifications.push({
        symbol,
        interval,
        pctChange,
        absPctChange,
      });
    }
  }

  return notifications;
};

const createNotificationMessage = (data) => ({
  symbol: data.symbol,
  message: `Price ${
    data.pctChange > 0 ? "increased" : "decreased"
  } by ${data.absPctChange.toFixed(2)}% in the last ${
    data.interval
  } minute(s)!`,
});

const checkForNotifications = () => {
  const stockSymbols = Array.from(currentBuffer.keys());

  stockSymbols.forEach((symbol) => {
    const stockData = currentBuffer.get(symbol);
    const thresholdDataList = checkStockThresholds(symbol, stockData);

    thresholdDataList.forEach((thresholdData) => {
      const notification = createNotificationMessage(thresholdData);
      notificationQueue.push(notification);
      console.log(`Enqueued notification for ${symbol}:`, notification);
    });
  });
};

const processNotificationQueue = (res) => {
  if (notificationQueue.length > 0) {
    // Process up to MAX_NOTIFICATIONS_PER_BATCH per dispatch interval
    for (
      let i = 0;
      i < Math.min(MAX_NOTIFICATIONS_PER_BATCH, notificationQueue.length);
      i++
    ) {
      const notification = notificationQueue.shift();
      res.write(`data: ${JSON.stringify(notification)}\n\n`);
      console.log("Dispatched notification:", notification);
    }
  }
};

const notification = (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const checkIntervalId = setInterval(
    checkForNotifications,
    NOTIFICATION_CHECK_INTERVAL
  );
  const dispatchIntervalId = setInterval(
    () => processNotificationQueue(res),
    NOTIFICATION_DISPATCH_INTERVAL
  );

  // Initial check
  checkForNotifications();

  req.on("close", () => {
    clearInterval(checkIntervalId);
    clearInterval(dispatchIntervalId);
    res.end();
    console.log("SSE client disconnected");
  });
};

module.exports = { notification };
