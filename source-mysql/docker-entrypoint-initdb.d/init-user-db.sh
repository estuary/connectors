#!/bin/bash
set -e

echo "======================================="
echo "Initializing Database for Flow Captures"
echo "======================================="

mysql --user="root" --password="secret1234" --database="mysql" <<-EOSQL
    CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234';

    CREATE DATABASE IF NOT EXISTS test;

    GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
    GRANT SELECT ON *.* TO 'flow_capture';

    CREATE DATABASE IF NOT EXISTS flow;
    CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
    GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';
EOSQL
