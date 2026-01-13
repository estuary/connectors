#!/bin/sh
set -ex
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
EXECUTE sp_configure 'max text repl size', -1;
GO
RECONFIGURE;
GO
ALTER DATABASE test SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO
CREATE LOGIN flow_capture WITH PASSWORD = 'we2rie1E';
GO
CREATE USER flow_capture FOR LOGIN flow_capture;
GO
GRANT VIEW DATABASE STATE TO flow_capture;
GO
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GO
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
GO
GRANT ALTER ANY SCHEMA TO flow_capture;
GO
  " | /opt/mssql-tools18/bin/sqlcmd -C -U sa -P gf6w6dkD
  echo "[initdb] Database initialization complete!"
  touch /var/opt/mssql/initdb-performed
else
  echo "[initdb] Database previously initialized"
fi

wait ${DBPID}
