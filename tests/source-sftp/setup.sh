#!/bin/bash

set -e
export SFTP_ADDRESS="${SFTP_ADDRESS:=source-sftp-db-1.flow-test:2222}"
export SFTP_USERNAME="${SFTP_USERNAME:=test}"
export SFTP_PASSWORD="${SFTP_PASSWORD:=test}"
export TEST_STREAM="${TEST_STREAM:=/tests}"

# Set ID_TYPE to string because parsing CSV files will always result in string values.
export ID_TYPE=string

export RESOURCE="{ \"stream\": \"${TEST_STREAM}\", \"syncMode\": \"incremental\" }"

docker compose -f source-sftp/docker-compose.yaml up --detach

# Wait for the sftp server to be ready, and learn the host key it generated on startup so the
# connector can be pinned to it. The prober reaches the server as localhost:2222 while the
# connector reaches it as ${SFTP_ADDRESS} inside the docker network, so the known_hosts line is
# written for the latter.
SFTP_HOST_KEY="$(go run tests/source-sftp/wait_for_server.go --username test --password test --address localhost:2222 --known-hosts-address "${SFTP_ADDRESS}")"
if [[ -z "${SFTP_HOST_KEY}" ]]; then
  echo "wait_for_server produced no host key; refusing to run without host key verification" >&2
  exit 1
fi
export SFTP_HOST_KEY
echo "SFTP server host key is: ${SFTP_HOST_KEY}"

# The config excludes JSON files by only including CSV files, since the JSON data has the "id"
# property as an integer.
config_json_template='{
    "address": "${SFTP_ADDRESS}",
    "knownHosts": "${SFTP_HOST_KEY}",
    "username": "${SFTP_USERNAME}",
    "password": "${SFTP_PASSWORD}",
    "directory": "${TEST_STREAM}",
    "matchFiles": ".*csv.*"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".
