Flow Batch SQL Source Connector
===============================

This is a connector which periodically executes SQL `SELECT` queries and
emits the resulting rows into Flow as JSON documents. It is designed to
be a flexible but not particularly point-and-click friendly tool, however
if extended with suitable discovery logic it could become much more usable.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-batchsql:local -f source-batchsql/Dockerfile .
    $ flowctl raw capture acmeCo/flow.yaml

Example catalog:

    captures:
      acmeCo/something/source-batchsql:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-batchsql:local"
            config:
              address: "localhost:5432"
              database: "postgres"
              password: "secret1234"
              user: "postgres"
        bindings:
          - resource:
              name: "foobar"
              template: >
                SELECT xmin, * FROM public.foobar
                {{if .subsequent}}
                    WHERE xmin::text::bigint > $1 ORDER BY xmin::text::bigint
                {{end}}
              cursor: ["xmin"]
              poll: 5s
            target: "acmeCo/something/foobar"
    
    collections:
      acmeCo/something/foobar:
        key: [/xmin]
        schema:
          properties:
            xmin: {type: integer}
          required: [xmin]
          type: object