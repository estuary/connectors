#!/bin/bash -e

echo "Deleting kinesis stream: '$STREAM'"
aws kinesis delete-stream --stream-name "$STREAM"
echo "Successfully deleted kinesis stream"
