#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

aws glue delete-database --name "${NAMESPACE}"
aws s3 rm $S3_DATA_URI --recursive
