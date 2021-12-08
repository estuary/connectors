#!/bin/bash

echo "Cleanup, cleanup!"

function sql {
    echo "psql> " $@
    psql -c "$@"
}

sql "DROP TABLE IF EXISTS ${TEST_STREAM};"