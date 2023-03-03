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
CREATE LOGIN flow_capture WITH PASSWORD = 'we2rie1E';
GO
CREATE USER flow_capture FOR LOGIN flow_capture;
GO
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GO
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
GO
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GO
GRANT INSERT, UPDATE ON dbo.flow_watermarks TO flow_capture;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'flow_capture';
GO
  " | /opt/mssql-tools/bin/sqlcmd -U sa -P gf6w6dkD
  echo "[initdb] Database initialization complete!"
  touch /var/opt/mssql/initdb-performed
else
  echo "[initdb] Database previously initialized"
fi

wait ${DBPID}