Flow Source Connector: MS SQL Server
====================================

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/)
which captures change events from a Microsoft SQL Server database.

TODO(wgd): Describe the details of this connector in more depth once it's written.

    $ docker-compose exec db /opt/mssql-tools/bin/sqlcmd -U sa -P gf6w6dkD

    1> SELECT * FROM sysobjects WHERE name = 'foobar';
    2> go
    name                                                                                                                             id          xtype uid    info   status      base_schema_ver replinfo    parent_obj  crdate                  ftcatid schema_ver  stats_schema_ver type userstat sysstat indexdel refdate                 version     deltrig     instrig     updtrig     seltrig     category    cache 
    -------------------------------------------------------------------------------------------------------------------------------- ----------- ----- ------ ------ ----------- --------------- ----------- ----------- ----------------------- ------- ----------- ---------------- ---- -------- ------- -------- ----------------------- ----------- ----------- ----------- ----------- ----------- ----------- ------
    foobar                                                                                                                             631673298 U          1      0           0               0           0           0 2023-01-05 20:02:30.960       0           0                0 U           1       3        0 2023-01-05 20:02:30.960           0           0           0           0           0           0      0

