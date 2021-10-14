#!/bin/bash
set -e

if [[ -n "${TEST_BUCKET}" ]]; then
    echo "Cleaning up bucket: '${TEST_BUCKET}'"
    aws s3 rb "s3://${TEST_BUCKET}" --force
fi