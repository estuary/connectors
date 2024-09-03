#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
    prefix=$(curl -s "http://localhost:8090/catalog/v1/config?warehouse=${WAREHOUSE}" | jq '.overrides.prefix' | tr -d '"')
    table=$(curl http://localhost:8090/catalog/v1/${prefix}/namespaces/${NAMESPACE}/tables/${1})
    data_dir=$(echo $table | jq '.metadata.location' | tr -d '"')

    duckdb -json :memory: "SET timezone to UTC; \
        INSTALL httpfs; \
        LOAD httpfs; \
        SET s3_access_key_id='${AWS_ACCESS_KEY_ID}'; \
        SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}'; \
        SET s3_region='${AWS_REGION}'; \
        SELECT * from '${data_dir}/*.parquet' order by id;"
}

exportToJsonl "simple_delta"
