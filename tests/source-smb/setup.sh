#!/bin/bash

set -e

# The shared test runner uses envsubst, which is not installed by default on macOS.
if ! command -v envsubst >/dev/null 2>&1; then
  envsubst() {
    jq -Rjs '
      env as $env |
      gsub("\\$(\\{(?<braced>[A-Za-z_][A-Za-z_0-9]*)\\}|(?<bare>[A-Za-z_][A-Za-z_0-9]*))";
        $env[.braced // .bare] // "")
    '
  }
fi

export TEST_STREAM="/inbox"
export RESOURCE="{ \"stream\": \"${TEST_STREAM}\" }"

# Set ID_TYPE to string because parsing CSV files will always result in string values.
export ID_TYPE=string

export CONNECTOR_CONFIG='{
    "host": "source-smb-db-1.flow-test",
    "port": 445,
    "share": "data",
    "username": "test",
    "password": "test",
    "directory": "inbox",
    "matchFiles": ".*\\.csv$"
}'
export CONNECTOR_CONFIG="$(echo "$CONNECTOR_CONFIG" | jq -c)"

docker compose -f source-smb/docker-compose.yaml up --wait --wait-timeout 90

# Seed the local test share, including a matching file in a subdirectory and an excluded file.
docker compose -f source-smb/docker-compose.yaml exec -T db sh -eu <<'EOF'
mkdir -p /share/inbox/sub
printf 'id,canary\n1,a1\n2,a2\n3,a3\n' > /share/inbox/a.csv
printf 'id,canary\n4,b4\n5,b5\n' > /share/inbox/sub/b.csv
printf 'id,canary\n99,excluded\n' > /share/inbox/notes.txt
# Use old modification times so files are immediately eligible for capture.
touch -t 202001010000.00 /share/inbox/a.csv /share/inbox/sub/b.csv /share/inbox/notes.txt
chmod -R a+rX /share
EOF
