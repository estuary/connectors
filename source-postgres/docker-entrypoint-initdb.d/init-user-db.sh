#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
    GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
    CREATE PUBLICATION flow_publication FOR ALL TABLES;
    ALTER SYSTEM SET wal_level = logical;
EOSQL
