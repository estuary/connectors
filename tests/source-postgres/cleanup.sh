#!/bin/bash

echo "Cleanup, cleanup!"

function sql {
    echo "psql> " $@

    docker exec \
      -e PGUSER=$PGUSER \
      -e PGPASSWORD=$PGPASSWORD \
      -e PGDATABASE=$PGDATABASE \
      source-postgres-postgres-1 \
      psql -c "$@"
}

sql "DROP TABLE IF EXISTS ${TEST_STREAM};"
