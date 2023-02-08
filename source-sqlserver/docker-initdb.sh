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
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'sa', @capture_instance = 'dbo_flow_watermarks';
GO
INSERT INTO dbo.flow_watermarks VALUES (0, 'dummy-value');
GO
  " | /opt/mssql-tools/bin/sqlcmd -U sa -P gf6w6dkD
  echo "[initdb] Database initialization complete!"
  touch /var/opt/mssql/initdb-performed
else
  echo "[initdb] Database previously initialized"
fi

wait ${DBPID}