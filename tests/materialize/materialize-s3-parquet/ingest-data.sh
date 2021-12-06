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

sleep 5
go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_SIMPLE} \
  --data_file_path=${DATASET_SIMPLE}