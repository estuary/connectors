#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER flow_capture WITH PASSWORD 'secret1234' REPLICATION;

    CREATE SCHEMA IF NOT EXISTS test;
    GRANT USAGE ON SCHEMA test TO flow_capture;
    ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES to flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA test TO flow_capture;

    GRANT CREATE, USAGE ON SCHEMA public TO flow_capture;

    CREATE PUBLICATION flow_publication FOR ALL TABLES;
EOSQL
