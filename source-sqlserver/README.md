Flow Source Connector: MS SQL Server
====================================

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/)
which captures change events from a Microsoft SQL Server database.

### Change Tables

Unlike many databases, SQL Server does not provide clients with direct access
to WAL events. Instead, it uses a "change tables" abstraction. Basically you
enable CDC on a particular source table, and this creates a destination "change table"
in the `cdc` schema as well as performing some other setup, and then the "Agent"
process will tail the writeahead log and periodically write change events into
the change table.

This is actually kind of nice, because it means that there's only one code path
along which value encoding/decoding/translation takes place. But it's also a pain
because:

  1. Waiting on the agent to write new events to the change table adds latency.
  2. We have to poll the change tables for new events periodically, and this also
     adds latency.

This is mostly an issue for small-scale testing, as these overheads are more or
less fixed and unrelated to the actual volume of changes. But this makes our
automated test suite runs take 10-20x longer than they do on other databases.

### Developing

Some useful commands for working with a test instance of SQL Server:

    $ docker-compose -f ./source-sqlserver/docker-compose.yaml exec db /opt/mssql-tools/bin/sqlcmd -U sa -P gf6w6dkD

    ## Enabling CDC on a database
    > CREATE DATABASE test;
    > GO
    > USE test;
    > GO
    > EXEC sys.sp_cdc_enable_db;
    > GO

    ## Enabling CDC on a specific table
    > CREATE TABLE foobar (id INTEGER PRIMARY KEY, data TEXT);
    > GO
    > EXEC sys.sp_cdc_help_change_data_capture;
    > GO
    > EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'foobar', @role_name = N'sa', @capture_instance = N'dbo_foobar';
    > GO