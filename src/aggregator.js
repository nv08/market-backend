"use strict";

const { pool } = require("./db");
const { from: copyFrom } = require("pg-copy-streams");
const { currentBuffer, flushingBuffer } = require("./memory");
const { Readable } = require("stream");
const { DB_INTERVALS, RUNNING_AGGREGATIONS_INTERVAL } = require("./constants");
const { colorize } = require("./helper");

const lastFlushedTimestamps = new Map();

async function flushToDatabase() {
  console.log(colorize.debug("Current buffer size before flush: " + currentBuffer.size));
  flushingBuffer.clear();

  const seenKeys = new Set();
  for (const [stock_symbol, stockData] of currentBuffer) {
    const lastFlushTs = lastFlushedTimestamps.get(stock_symbol) || 0;
    const uniqueData = [];
    const dataPoints = stockData.dataPoints || [];

    for (const d of dataPoints) {
      const key = `${stock_symbol}|${d.timestamp}`;
      if (d.timestamp > lastFlushTs && !seenKeys.has(key)) {
        seenKeys.add(key);
        uniqueData.push({ ...d });
      } else {
        // console.log(
        //   `Skipped ${key}: already flushed or duplicate in this batch`
        // );
      }
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
    console.time(colorize.info("flush"));

    // Check existing timestamps in DB
    const allKeys = [...seenKeys];
    const existingResult = await client.query(
      `SELECT stock_symbol || '|' || timestamp AS key 
       FROM stocks 
       WHERE stock_symbol || '|' || timestamp = ANY($1::text[])`,
      [allKeys]
    );
    const existingKeys = new Set(existingResult.rows.map((row) => row.key));
    // console.log("Existing keys in DB:", [...existingKeys]);

    const rows = [];
    let rowCount = 0;
    const maxTimestamps = new Map();

    for (const [stock_symbol, data] of flushingBuffer) {
      for (const d of data) {
        const key = `${stock_symbol}|${d.timestamp}`;
        if (!existingKeys.has(key)) {
          rows.push(
            `${stock_symbol},${d.cmp},${d.open},${d.high},${d.low},${d.close},${d.timestamp}\n`
          );
          rowCount++;
          const currentMax = maxTimestamps.get(stock_symbol) || 0;
          if (d.timestamp > currentMax) {
            maxTimestamps.set(stock_symbol, d.timestamp);
          }
        } else {
          console.log(`Skipped ${key}: already exists in DB`);
        }
      }
    }

    if (rowCount === 0) {
      console.log("No new rows to flush after DB check");
      flushingBuffer.clear();
      return;
    }

    console.log(colorize.info(`Flushing ${rowCount} rows`));

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
        console.timeEnd(colorize.info("flush"));
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
    console.log(colorize.error("Flush failed:", e.message));
    throw e;
  } finally {
    client.release();
  }
}

async function runAggregation(client, interval, intervalMs, nowMs) {
  // console.log(
  //   `Running aggregation for ${interval}-minute interval at ${new Date(
  //     nowMs
  //   ).toISOString()}`
  // );
  await client.query(
    `
    INSERT INTO stock_aggregates (stock_symbol, interval_minutes, timestamp_bucket, pct_change)
    SELECT 
      stock_symbol,
      $1::INTEGER AS interval_minutes,
      ($3::BIGINT - ($3::BIGINT % $2::BIGINT)) AS timestamp_bucket,
      ROUND(
        ((LAST(close, timestamp) - FIRST(close, timestamp)) / FIRST(close, timestamp)) * 100,
        2
      ) AS pct_change
    FROM stocks
    WHERE timestamp::BIGINT BETWEEN ($3::BIGINT - $2::BIGINT) AND $3::BIGINT
    GROUP BY stock_symbol
    ON CONFLICT (stock_symbol, interval_minutes, timestamp_bucket)
    DO UPDATE SET pct_change = EXCLUDED.pct_change;
    `,
    [interval, intervalMs, nowMs]
  );
}

function scheduleAggregations() {
  const aggregationInterval = RUNNING_AGGREGATIONS_INTERVAL
  DB_INTERVALS.forEach((minutes) => {
    setInterval(async () => {
      const client = await pool.connect();
      try {
        const nowMs = Math.floor(Date.now());
        await runAggregation(client, minutes, aggregationInterval, nowMs);
        console.log(colorize.success(`Aggregation completed for ${minutes}-minute interval`));
      } catch (e) {
        console.log(
          colorize.error(
            `Aggregation failed for ${minutes}-minute interval:`,
            e.message
          )
        );
      } finally {
        client.release();
      }
    }, aggregationInterval);
  });
}

module.exports = { flushToDatabase, scheduleAggregations };
