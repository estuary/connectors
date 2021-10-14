#!/bin/bash
set -e

# Sending corresponding data to the websockets.
websocat --protocol json/v1 "${WEBSOCKET_URL_SIMPLE}" < "${DATASET_SIMPLE}"

pv --line-mode --quiet --rate-limit 1 < "${DATASET_MULTIPLE_DATATYPES}" \
      | websocat --protocol json/v1 "${WEBSOCKET_URL_MULTIPLE_DATATYPES}"

sleep 5

websocat --protocol json/v1 "${WEBSOCKET_URL_SIMPLE}" < "${DATASET_SIMPLE}" 

# Wait long enough to allow all the data to be committed to the cloud.
sleep 5

