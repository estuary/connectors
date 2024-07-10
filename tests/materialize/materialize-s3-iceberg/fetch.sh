#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

duckdb -json :memory: "SET timezone to UTC; \
    INSTALL httpfs; \
    LOAD httpfs; \
    SET s3_access_key_id='${AWS_ACCESS_KEY_ID}'; \
    SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}'; \
    SET s3_region='${AWS_REGION}'; \
    SELECT * from '${S3_DATA_URI}/*.parquet' order by id;"
