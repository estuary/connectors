Flow Batch SQL Source Connector
===============================

This is a connector which periodically executes SQL `SELECT` queries and
emits the resulting rows into Flow as JSON documents. It is designed to
be a flexible but not particularly point-and-click friendly tool, however
if extended with suitable discovery logic it could become much more usable.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-oracle-batch:local -f source-oracle-batch/Dockerfile .
    $ flowctl raw capture acmeCo/flow.yaml

Example catalog:

    captures:
      acmeCo/something/source-oracle-batch:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-oracle-batch:local"
            config:
              address: "localhost:5432"
              database: "oracle"
              password: "secret1234"
              user: "oracle"
        bindings:
          - resource:
              name: "foobar"
              template: >
                {{if .IsFirstQuery -}}
                  SELECT ORA_ROWSCN AS TXID, {{quoteTableName .Owner .TableName}}.* FROM {{quoteTableName .Owner .TableName}} ORDER BY ORA_ROWSCN
                {{- else -}}
                  SELECT ORA_ROWSCN AS TXID, {{quoteTableName .Owner .TableName}}.* FROM {{quoteTableName .Owner .TableName}}
                    WHERE ORA_ROWSCN > :1
                    ORDER BY ORA_ROWSCN
                {{- end}}
              cursor: ["TXID"]
              poll: 5s
            target: "acmeCo/something/foobar"
    
    collections:
      acmeCo/something/foobar:
        key: [/_meta/polled, /_meta/index]
        schema:
          properties:
            _meta:
              type: object
              properties:
                polled: {type: string, format: date-time}
                index: {type: integer}
              required: [polled, index]
          required: [_meta]
          type: object
