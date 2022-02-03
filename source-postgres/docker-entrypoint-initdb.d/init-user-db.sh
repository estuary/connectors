#!/bin/bash
set -e

# The alter system command at the end is redundant due command line settings.
#  Included as an example of how to set this without using the command line.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER flow_capture WITH PASSWORD 'flow_capture' REPLICATION;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
    GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
    CREATE PUBLICATION flow_publication FOR ALL TABLES;
    ALTER SYSTEM SET wal_level = logical;
EOSQL
