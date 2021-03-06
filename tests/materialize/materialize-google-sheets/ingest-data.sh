#!/bin/bash
set -e

go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_SIMPLE} \
  --data_file_path=${DATASET_SIMPLE}

go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_DUPLICATED_KEYS} \
  --data_file_path=${DATASET_DUPLICATED_KEYS}

go run ${DATA_INGEST_SCRIPT_PATH} \
  --server_address=${CONSUMER_ADDRESS} \
  --capture=${PUSH_CAPTURE_NAME} \
  --binding_num=${BINDING_NUM_MULTIPLE_DATATYPES} \
  --data_file_path=${DATASET_MULTIPLE_DATATYPES}