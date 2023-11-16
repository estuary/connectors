Flow Batch BigQuery Source Connector
====================================

This is a connector which periodically executes SQL `SELECT` queries and
emits the resulting rows into Flow as JSON documents. It is designed to
be a flexible but not particularly point-and-click friendly tool, however
it also implements table discovery logic which should work in most simple
cases.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-bigquery-batch:local -f source-bigquery-batch/Dockerfile .
    $ flowctl raw discover --source acmeCo/flow.yaml
    $ flowctl raw capture acmeCo/flow.yaml

Example `flow.yaml` for discovery:

    captures:
      acmeCo/source-bigquery-batch:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-bigquery-batch:local"
            config:
              project_id: pivotal-base-360421
              credentials_json: "<REDACTED>"
              advanced:
                poll: 5s
        bindings: []

And a fleshed-out catalog with a discovery binding:

    captures:
      acmeCo/source-bigquery-batch:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-bigquery-batch:local"
            config:
              project_id: pivotal-base-360421
              credentials_json: "<REDACTED>"
              dataset: testdata
              advanced:
                poll: 5s
        bindings:
          - resource:
              name: foobar
              template: "SELECT * FROM testdata.foobar;"
            target: acmeCo/foobar