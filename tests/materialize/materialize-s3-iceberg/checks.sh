#!/bin/bash

SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi

# File keys are represented in the connector state as random UUIDs and will be
# replaced with a placeholder to make the test reproducable.
$SED_CMD -i'' 's/[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}.parquet/<UUID>.parquet/g' ${SNAPSHOT}
