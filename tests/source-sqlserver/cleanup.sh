#!/bin/bash

echo "Undoing database test setup"

function sql {
    echo "sql> " $@
    echo -e "$@\nGO\n" | docker exec -i \
      ${CONTAINER_NAME} \
      /opt/mssql-tools18/bin/sqlcmd -C \
      -U ${SQLSERVER_USER} \
      -P ${SQLSERVER_PASSWORD}
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"
