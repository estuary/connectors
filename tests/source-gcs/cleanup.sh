#!/bin/bash

set -e

if [[ -n "$TEST_STREAM" ]]; then
    echo "deleting bucket $TEST_STREAM"
    gsutil rm -r "gs://$TEST_STREAM"
    echo "successfully deleted bucket"
fi
