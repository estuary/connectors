Flow Batch SQL Source Connector
===============================

This is a connector which periodically executes SQL `SELECT` queries and
emits the resulting rows into Flow as JSON documents. It is designed to
be a flexible but not particularly point-and-click friendly tool, however
if extended with suitable discovery logic it could become much more usable.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-sqlserver-batch:local -f source-sqlserver-batch/Dockerfile .
    $ flowctl raw discover --source acmeCo/flow.yaml
    $ flowctl raw capture acmeCo/flow.yaml

Example `flow.yaml` for discovery:

    ---
    captures:
      acmeCo/source-mysql-batch:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-mysql-batch:local"
            config:
              address: "127.0.0.1:1433"
              database: test
              password: we2rie1E
              user: flow_capture
        bindings: []
