#!/bin/bash

echo "Undoing database test setup"

function sql {
    echo "mysql> " $@
    echo "$@" | docker exec -i \
      source-mysql-db-1 \
      mysql $MYSQL_DATABASE --user=$MYSQL_USER --local-infile=1
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"
