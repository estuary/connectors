#!/bin/bash

echo "Cleanup, cleanup!"

function sql {
    echo "psql> " $@
    psql "${POSTGRES_CONNECTION_URI}" -c "$@"
}

sql "DROP TABLE IF EXISTS ${TEST_STREAM};"