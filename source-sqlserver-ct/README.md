Flow Source Connector: MS SQL Server (Change Tracking)
======================================================

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/)
which captures change events from a Microsoft SQL Server database using
SQL Server's Change Tracking feature.

### Change Tracking

Change Tracking is a lightweight synchronous mechanism built into SQL Server
that tracks which rows have changed in a table. Unlike CDC (Change Data Capture),
which uses the transaction log and an asynchronous agent process, Change Tracking:

  1. Records changes synchronously as part of each transaction
  2. Only tracks that a row changed (insert/update/delete) and provides the primary key
  3. Does not store the actual changed values - those are fetched by joining with the source table
  4. Has lower overhead and simpler setup than CDC
  5. Requires tables to have a primary key

The connector polls for changes using the `CHANGETABLE(CHANGES ...)` function and
joins with the source table to get the current row values for inserts and updates.

### Developing

Some useful commands for working with a test instance of SQL Server:

    $ docker-compose -f ./source-sqlserver-ct/docker-compose.yaml exec db /opt/mssql-tools18/bin/sqlcmd -C -U sa -P gf6w6dkD

    ## Enabling Change Tracking on a database
    > CREATE DATABASE test;
    > GO
    > USE test;
    > GO
    > ALTER DATABASE test SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
    > GO

    ## Enabling Change Tracking on a specific table
    > CREATE TABLE foobar (id INTEGER PRIMARY KEY, data TEXT);
    > GO
    > ALTER TABLE foobar ENABLE CHANGE_TRACKING;
    > GO

    ## Querying the current Change Tracking version
    > SELECT CHANGE_TRACKING_CURRENT_VERSION();
    > GO

Building connector images:

    $ docker build --network=flow-test -t source-sqlserver-ct:local -f source-sqlserver-ct/Dockerfile .
