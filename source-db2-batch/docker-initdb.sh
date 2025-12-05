#!/bin/bash
set -e

# This script runs as part of the DB2 container initialization
# It creates a test user with appropriate permissions for connector testing

echo "[initdb] Starting DB2 initialization script..."

# Wait for DB2 to be ready
until su - db2inst1 -c "db2 connect to testdb" > /dev/null 2>&1; do
  echo "[initdb] Waiting for DB2 to be ready..."
  sleep 5
done

echo "[initdb] DB2 is ready, setting up test schema and users..."

# Create a less-privileged flow_capture OS user for connector testing.
# DB2 uses OS-level authentication, so we must create the user at the OS level first.
if ! id flow_capture &>/dev/null; then
  useradd -m flow_capture
  echo 'flow_capture:secret1234' | chpasswd
  echo "[initdb] Created flow_capture OS user"
else
  echo "[initdb] flow_capture OS user already exists"
fi

# Run DB2 commands one at a time without semicolons in interactive mode
su - db2inst1 -c "
db2 connect to testdb

# Create test schema
db2 'CREATE SCHEMA test'
db2 'CREATE TABLE test.simple_test (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
db2 'INSERT INTO test.simple_test (id, name) VALUES (1, '\''Initial Test Row'\'')'

# Grant flow_capture user the minimum privileges needed for capture:
# - CONNECT allows connecting to the database
# - SELECTIN allows reading from all tables in the schema (existing and future)
db2 'GRANT CONNECT ON DATABASE TO USER flow_capture'
db2 'GRANT SELECTIN ON SCHEMA test TO USER flow_capture'

db2 commit
db2 connect reset
"

echo "[initdb] DB2 initialization complete!"
