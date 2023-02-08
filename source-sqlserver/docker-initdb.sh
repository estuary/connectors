#!/bin/sh
/opt/mssql/bin/sqlservr &
DBPID="$!"

if [ ! -e /var/opt/mssql/initdb-performed ]; then
  echo "[initdb] Waiting to initialize database..."
  sleep 10
  echo "[initdb] Initializing database..."
  echo "
CREATE DATABASE test;
GO
USE test;
GO
EXEC sys.sp_cdc_enable_db;
GO
CREATE TABLE dummy_table (id INTEGER PRIMARY KEY, data TEXT);
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy_table', @role_name = 'sa';
GO
INSERT INTO dummy_table VALUES (0, 'hello'), (1, 'world');
GO
  " | /opt/mssql-tools/bin/sqlcmd -U sa -P gf6w6dkD
  echo "[initdb] Database initialzation complete!"
  touch /var/opt/mssql/initdb-performed
else
  echo "[initdb] Database previously initialized"
fi

wait ${DBPID}