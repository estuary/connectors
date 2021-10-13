#!/bin/bash
set -e
if [ $# -ne 2 ] 
then
    echo "execution using: $0 <tmp-dir> <test-output-jsonl-dir>"
    exit 1
fi

tmp_dir="$1"
test_output_jsonl_dir="$2"

# Sync data to local.
aws s3 sync s3://"${TEST_BUCKET}" "${tmp_dir}" \
    || bail "syncing data from s3 failed"

# Read all the pq data as jsonl output.
function exportParquetToJson() {
    local pq_path=$1
    local jsonl_path=$2

    # Sort the files by names, in the order of
    # xx_0.pq, xx_1.pq, ... xx_9.pq, xx_10.pq, xx_11.pq, ...   
    pq_files=$(ls -A "${pq_path}" | sort | awk '{ print length, $0 }' | sort -n -s  | cut -d" " -f2-)

    if [[ -z "${pq_files}" ]]; then
        bail "no parquet file is generated."
    fi

    for pq_file in ${pq_files}; do
        docker run --rm -it -v "${pq_path}":/data \
           nathanhowell/parquet-tools cat -json /data/"${pq_file}" >> "${jsonl_path}" \
           || bail "generating jsonl failed"
 
    done
}

exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_SIMPLE}" "${tmp_dir}/simple.jsonl"
combineResults "${TEST_COLLECTION_SIMPLE}" "${tmp_dir}/simple.jsonl" "${test_output_jsonl_dir}/simple.jsonl"

exportParquetToJson "${tmp_dir}/${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}"  "${tmp_dir}/multiple_datatypes.jsonl"
combineResults "${TEST_COLLECTION_MULTIPLE_DATATYPES}" "${tmp_dir}/multiple_datatypes.jsonl" "${test_output_jsonl_dir}/multiple_datatypes.jsonl"