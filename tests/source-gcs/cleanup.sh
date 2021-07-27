#!/bin/bash

set -e

if [[ -n "$TEST_BUCKET" ]]; then
    echo "deleting bucket $TEST_BUCKET"
    gsutil rm -r "gs://$TEST_BUCKET"
    echo "successfully deleted bucket"
fi
