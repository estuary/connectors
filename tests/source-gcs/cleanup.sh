#!/bin/bash

set -e

echo "deleting bucket $STREAM"
gsutil rm -r "gs://$STREAM"
echo "successfully deleted bucket"
