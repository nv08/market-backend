"use strict";

const { pool } = require("./db");
const { from: copyFrom } = require("pg-copy-streams");
const { currentBuffer, flushingBuffer } = require("./memory");
const { Readable } = require("stream");

const lastFlushedTimestamps = new Map();

async function flushToDatabase() {
  console.log("Current buffer size before flush:", currentBuffer.size);

  flushingBuffer.clear();

  const seenKeys = new Set();
  for (const [stock_symbol, data] of currentBuffer) {
    const lastFlushTs = lastFlushedTimestamps.get(stock_symbol) || 0;
    const uniqueData = [];
    for (const d of data) {
      const key = `${stock_symbol}|${d.timestamp}`;
      if (d.timestamp > lastFlushTs && !seenKeys.has(key)) {
        seenKeys.add(key);
        uniqueData.push({ ...d });
      }
      // else if (d.timestamp <= lastFlushTs) {
      //   console.log(`Skipped already flushed: ${key}`);
      // } else {
      //   console.log(`Duplicate found and skipped: ${key}`);
      // }
    }
    if (uniqueData.length > 0) {
      flushingBuffer.set(stock_symbol, uniqueData);
    }
  }

  if (
    flushingBuffer.size === 0 ||
    [...flushingBuffer.values()].every((arr) => arr.length === 0)
  ) {
    console.log("No new unique data to flush");
    return;
  }

  const client = await pool.connect();
  try {
    console.time("flush");

    const rows = [];
    let rowCount = 0;
    let maxTimestamps = new Map();
    for (const [stock_symbol, data] of flushingBuffer) {
      for (const d of data) {
        rows.push(
          `${stock_symbol},${d.cmp},${d.open},${d.high},${d.low},${d.close},${d.timestamp}\n`
        );
        rowCount++;
        const currentMax = maxTimestamps.get(stock_symbol) || 0;
        if (d.timestamp > currentMax) {
          maxTimestamps.set(stock_symbol, d.timestamp);
        }
      }
    }

    const readableStream = Readable.from(rows);
    const copyStream = client.query(
      copyFrom(`
        COPY stocks (stock_symbol, cmp, open, high, low, close, timestamp)
        FROM STDIN WITH (FORMAT CSV)
      `)
    );

    readableStream.pipe(copyStream);

    await new Promise((resolve, reject) => {
      copyStream.on("finish", () => {
        console.timeEnd("flush");
        console.log(
          `Flushed ${rowCount} rows across ${flushingBuffer.size} stocks to database`
        );
        for (const [stock_symbol, maxTs] of maxTimestamps) {
          lastFlushedTimestamps.set(stock_symbol, maxTs);
        }
        resolve();
      });
      copyStream.on("error", (err) => {
        console.error("Stream error:", err.message);
        reject(err);
      });
    });

    flushingBuffer.clear();
  } catch (e) {
    console.error("Flush failed:", e.message);
    throw e;
  } finally {
    client.release();
  }
}

async function runAggregations() {
  const intervals = [10, 15, 30];
  const client = await pool.connect();
  try {
    const nowMs = Math.floor(Date.now());
    const earliestData = await client.query(
      "SELECT MIN(timestamp) AS min_ts FROM stocks"
    );
    const minTs = earliestData.rows[0]?.min_ts || nowMs;
    const dataSpan = nowMs - minTs;
    console.log(`Data span: ${dataSpan / 60000} minutes`);

    for (const interval of intervals) {
      const intervalMs = interval * 60 * 1000;
      console.log(
        `Running aggregation for interval: ${interval}, intervalMs: ${intervalMs}, nowMs: ${nowMs}`
      );

      // Calculate pct_change over the full interval window
      await client.query(
        `
        INSERT INTO stock_aggregates (stock_symbol, interval_minutes, timestamp_bucket, pct_change)
        SELECT 
          stock_symbol,
          $1::INTEGER AS interval_minutes,
          $3::BIGINT - ($3::BIGINT % $2::BIGINT) AS timestamp_bucket,
          ROUND(
            ((LAST(close, timestamp) - FIRST(close, timestamp)) / FIRST(close, timestamp)) * 100,
            2
          ) AS pct_change
        FROM stocks
        WHERE timestamp::BIGINT BETWEEN $3::BIGINT - $2::BIGINT AND $3::BIGINT
        GROUP BY stock_symbol
        ON CONFLICT (stock_symbol, interval_minutes, timestamp_bucket)
        DO UPDATE SET pct_change = EXCLUDED.pct_change;
      `,
        [interval, intervalMs, nowMs]
      );
    }
    console.log("Aggregations updated for intervals:", intervals);
  } catch (e) {
    console.error("Aggregation failed:", e.message);
  } finally {
    client.release();
  }
}

module.exports = { flushToDatabase, runAggregations };
