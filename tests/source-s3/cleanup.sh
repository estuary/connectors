#!/bin/bash

echo "Cleaning up bucket: '$STREAM'"
aws s3 rb "s3://${STREAM}" --force

