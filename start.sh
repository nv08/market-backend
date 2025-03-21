#!/bin/bash
if [ ! -s "/var/lib/postgresql/data/PG_VERSION" ]; then
  su - postgres -c "initdb -D /var/lib/postgresql/data"
  # Add pg_hba.conf entries to allow local connections
  echo "host all all 172.17.0.0/16 trust" >> /var/lib/postgresql/data/pg_hba.conf
  echo "host all all 127.0.0.1/32 trust" >> /var/lib/postgresql/data/pg_hba.conf
  su - postgres -c "pg_ctl -D /var/lib/postgresql/data -l /tmp/pg.log start"
  until pg_isready -U postgres; do
    sleep 1
  done
  psql -U postgres -c "CREATE DATABASE stock_data;"
  psql -U postgres -d stock_data -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
  su - postgres -c "pg_ctl -D /var/lib/postgresql/data stop"
fi

su - postgres -c "postgres -D /var/lib/postgresql/data" &
until pg_isready -U postgres; do
  sleep 1
done
npm start