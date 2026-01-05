#!/bin/bash
set -e

echo "======================================="
echo "Initializing Database for Flow Captures"
echo "======================================="

# Use mariadb client if available, otherwise fall back to mysql
MYSQL_CLIENT=$(command -v mariadb || command -v mysql)

$MYSQL_CLIENT --user="root" --password="secret1234" --database="mysql" <<-EOSQL
    CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234';

    CREATE DATABASE IF NOT EXISTS test;

    GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
    GRANT SELECT ON *.* TO 'flow_capture';
EOSQL
