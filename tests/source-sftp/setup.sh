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

# Wait for the sftp server to be ready.
go run tests/source-sftp/wait_for_server.go

# The config excludes JSON files by only including CSV files, since the JSON data has the "id"
# property as an integer.
config_json_template='{
    "address": "${SFTP_ADDRESS}",
    "username": "${SFTP_USERNAME}",
    "password": "${SFTP_PASSWORD}",
    "directory": "${TEST_STREAM}",
    "matchFiles": ".*csv.*"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".
