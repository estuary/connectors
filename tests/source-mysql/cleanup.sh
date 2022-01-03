#!/bin/bash

echo "Undoing database test setup"

function sql {
    echo "mysql> " $@
    echo "$@" | mysql $MYSQL_DATABASE --user=$MYSQL_USER --local-infile=1
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"