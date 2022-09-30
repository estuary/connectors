#!/bin/bash

if [[ -n "$TEST_STREAM" ]]; then
    echo "Cleaning up bucket: '$TEST_STREAM'"
    aws s3 rb "s3://${TEST_STREAM}" --force
fi

