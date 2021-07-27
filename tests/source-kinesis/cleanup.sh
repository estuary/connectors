#!/bin/bash -e

echo "Deleting kinesis stream: '$TEST_STREAM'"
aws kinesis delete-stream --stream-name "$TEST_STREAM"
echo "Successfully deleted kinesis stream"
