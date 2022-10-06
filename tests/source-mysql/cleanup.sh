#!/bin/bash

echo "Undoing database test setup"

function sql {
    echo "mysql> " $@
    echo "$@" | docker exec -i \
      source-mysql-db-1 \
      mysql $MYSQL_DATABASE \
      --user=$MYSQL_USER \
      --port=$MYSQL_PORT \
      --host=$MYSQL_HOST \
      --password=$MYSQL_PASSWORD \
      --local-infile=1
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"
