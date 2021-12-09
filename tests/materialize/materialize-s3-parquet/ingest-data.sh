#!/bin/bash
set -e

# Pushing data to the capture.
go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_SIMPLE} \
  --data_file_path=${DATASET_SIMPLE}

go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_MULTIPLE_DATATYPES} \
  --data_file_path=${DATASET_MULTIPLE_DATATYPES}

# Wait long enough so that the temp data in the local caches are all committed to S3.
# Following data will be stored in a new local file.
sleep 5
go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_SIMPLE} \
  --data_file_path=${DATASET_SIMPLE}