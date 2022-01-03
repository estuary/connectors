#!/bin/bash
set -e

if [[ -n "${TEST_BUCKET}" && -n "${TEST_ID}" ]]; then
    echo "removing files in s3://${TEST_BUCKET}/${TEST_ID}"
    aws s3 rm --recursive "s3://${TEST_BUCKET}/${TEST_ID}"
fi
