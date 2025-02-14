#!/bin/bash
set -e

echo "======================================="
echo "Initializing Database for Flow Captures"
echo "======================================="

mysql --user="root" --password="secret1234" --database="mysql" <<-EOSQL
    CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234';
    CREATE DATABASE IF NOT EXISTS test;
    GRANT SELECT ON *.* TO 'flow_capture';
EOSQL
