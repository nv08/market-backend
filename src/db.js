"use strict";

const { Pool } = require("pg");
const { colorize } = require("./helper");
require("dotenv").config();

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

const setupDatabase = async () => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(`
      CREATE TABLE IF NOT EXISTS stocks (
        stock_symbol TEXT NOT NULL,
        cmp DECIMAL(10, 2) NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        timestamp BIGINT NOT NULL,
        PRIMARY KEY (stock_symbol, timestamp)
      );
    `);
    await client.query(`
      SELECT create_hypertable('stocks', 'timestamp', if_not_exists := TRUE, chunk_time_interval => 1800000);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS stocks_symbol_time_idx ON stocks (stock_symbol, timestamp DESC);
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stock_aggregates (
        stock_symbol TEXT NOT NULL,
        interval_minutes INT NOT NULL,
        timestamp_bucket BIGINT NOT NULL,
        pct_change DECIMAL(10, 4),
        PRIMARY KEY (stock_symbol, interval_minutes, timestamp_bucket)
      );
    `);

    // New table for active subscriptions
    await client.query(`
      CREATE TABLE IF NOT EXISTS stock_subscriptions (
        stock_symbol VARCHAR(16) PRIMARY KEY,
        subscribed_at BIGINT NOT NULL
      );
    `);

    await client.query("COMMIT");
    console.log(
      colorize.info(
      "Database setup completed: hypertable and aggregates table created.")
    );
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error setting up database:", error.message);
  } finally {
    client.release();
  }
};

module.exports = { setupDatabase, pool };
