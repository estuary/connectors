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
CREATE LOGIN flow_capture WITH PASSWORD = 'we2rie1E';
GO
CREATE USER flow_capture FOR LOGIN flow_capture;
GO
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GO
  " | /opt/mssql-tools18/bin/sqlcmd -C -U sa -P gf6w6dkD
  echo "[initdb] Database initialization complete!"
  touch /var/opt/mssql/initdb-performed
else
  echo "[initdb] Database previously initialized"
fi

wait ${DBPID}