const { currentBuffer, computePctChange } = require("../memory");

// Notification thresholds per interval (in minutes)
const notificationThresholds = {
  0: 0.50, // 0.50%
  1: 0.75, // 0.75%
  2: 1.00, // 1%
  3: 1.25, // 1.25%
  4: 1.50, // 1.50%
  5: 1.75, // 1.75%
};

// Queue to hold notifications
const notificationQueue = [];

// Function to check for notifications and enqueue them
const checkForNotifications = () => {
  const stockSymbols = Array.from(currentBuffer.keys());

  stockSymbols.forEach((symbol) => {
    const stockData = currentBuffer.get(symbol) || {
      dataPoints: [],
      latestClose: null,
    };

    // Skip if no data available
    if (!stockData.latestClose || stockData.dataPoints.length === 0) {
      return;
    }

    // Check intervals in ascending order (0 to 5) and notify only the first breach
    for (const intervalStr of Object.keys(notificationThresholds)) {
      const interval = parseInt(intervalStr, 10);
      const threshold = notificationThresholds[interval];
      const pctChange = computePctChange(symbol, interval);

      // Skip if pctChange is "N/A"
      if (pctChange === "N/A") {
        continue;
      }

      const absPctChange = Math.abs(pctChange);
      if (absPctChange >= threshold) {
        const direction = pctChange > 0 ? "increased" : "decreased";
        const eventData = {
          symbol,
          message: `price ${direction} by ${absPctChange.toFixed(2)}% in the last ${interval} minute(s)!`,
        };
        notificationQueue.push(eventData);
        console.log(`Enqueued notification for ${symbol}: ${JSON.stringify(eventData)}`);
        break; // Stop checking further intervals for this stock
      }
    }
  });
};

// Function to process the queue and send notifications periodically
const processNotificationQueue = (res) => {
  if (notificationQueue.length > 0) {
    const eventData = notificationQueue.shift(); // Take the first notification
    res.write(`data: ${JSON.stringify(eventData)}\n\n`);
    console.log(`Sent notification from queue: ${JSON.stringify(eventData)}`);
  }
};

// SSE endpoint
const notification = (req, res) => {
  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  // Check notifications every 15 seconds
  const checkIntervalId = setInterval(() => checkForNotifications(res), 15 * 1000);

  // Process queue every 5 seconds
  const queueIntervalId = setInterval(() => processNotificationQueue(res), 2 * 1000);

  // Initial check on connection
  checkForNotifications(res);

  // Clean up when client disconnects
  req.on("close", () => {
    clearInterval(checkIntervalId);
    clearInterval(queueIntervalId);
    res.end();
    console.log("SSE client disconnected");
  });
};

module.exports = { notification, checkForNotifications };