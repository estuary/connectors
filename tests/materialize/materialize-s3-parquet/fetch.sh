#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# The dir relative to ${TEMP_DIR} for storing temp files.
tmp_dir=tmp
mkdir -p $(realpath ${TEMP_DIR}/${tmp_dir})

# Sync data to local.
>&2 aws s3 sync "s3://${TEST_BUCKET}" ${TEMP_DIR}/${tmp_dir}/ --endpoint-url "${LOCALSTACK_S3_LOCAL_ENDPOINT}" \
    || bail "syncing data from s3 failed"

# Read all the pq data as jsonl output.
function exportParquetToJson() {
    local pq_path="${TEMP_DIR}"/"$1"

    # Sort the files by name, which should also correspond to the order in which
    # they were written.  We output paths relative to pq_path, and then set the
    # working dir of the docker container, so we don't have to translate
    # absolute paths to something that will work in the container.
    pq_files=$(cd "${pq_path}" && find . -type f -name '*.parquet' | sort )

    if [[ -z "${pq_files}" ]]; then
        bail "no parquet file is generated."
    fi
    >&2 echo "Parquet files: ${pq_files}"

    for pq_file in ${pq_files}; do
        docker run --rm -v "${pq_path}":/data \
           --workdir /data \
           nathanhowell/parquet-tools cat -json "${pq_file}"  \
           || bail "generating jsonl failed"
    done
}

exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_SIMPLE}"
exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}"
