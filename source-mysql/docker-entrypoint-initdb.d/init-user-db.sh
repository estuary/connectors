#!/bin/bash
set -e

echo "======================================="
echo "Initializing Database for Flow Captures"
echo "======================================="

mysql --user="root" --password="flow" --database="test" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS flow;
    CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
    CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret' COMMENT 'User account for Flow MySQL data capture';
    GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
    GRANT SELECT ON *.* TO 'flow_capture';
    GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';
    SET PERSIST binlog_row_metadata = 'FULL';
EOSQL
