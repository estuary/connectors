#!/bin/bash
set -e

if [ $# -ne 1 ]
then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TESTDIR} to store final results.
result_dir="$1"

# Wait long enough to have all the data uploaded to s3.
sleep 5

# The dir relative to ${TESTDIR} for storing temp files.
tmp_dir=tmp
mkdir -p "$(realpath "${TESTDIR}"/${tmp_dir})"

# Sync data to local.
aws s3 sync s3://"${TEST_BUCKET}/${TEST_ID}" "${TESTDIR}/${tmp_dir}/" \
    || bail "syncing data from s3 failed"

# Read all the pq data as jsonl output.
function exportParquetToJson() {
    local pq_path="${TESTDIR}"/"$1"
    local jsonl_path="${TESTDIR}"/"$2"

    # Sort the files by name, which should also correspond to the order in which
    # they were written.  We output paths relative to pq_path, and then set the
    # working dir of the docker container, so we don't have to translate
    # absolute paths to something that will work in the container.
    pq_files=$(cd "${pq_path}" && find . -type f -name '*.parquet' | sort )

    if [[ -z "${pq_files}" ]]; then
        bail "no parquet file is generated."
    fi
    echo -e "Parquet files: ${pq_files}"

    for pq_file in ${pq_files}; do
        docker run --rm -v "${pq_path}":/data \
           --workdir /data \
           nathanhowell/parquet-tools cat -json "${pq_file}" >> "${jsonl_path}" \
           || bail "generating jsonl failed"
 
    done
}

exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_SIMPLE}" "${tmp_dir}/simple.jsonl"
combineResults "${TEST_COLLECTION_SIMPLE}" "${tmp_dir}/simple.jsonl" "${result_dir}/simple.jsonl"

exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}"  "${tmp_dir}/multiple_datatypes.jsonl"
combineResults "${TEST_COLLECTION_MULTIPLE_DATATYPES}" "${tmp_dir}/multiple_datatypes.jsonl" "${result_dir}/multiple_datatypes.jsonl"