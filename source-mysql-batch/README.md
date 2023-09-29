Flow Batch SQL Source Connector
===============================

This is a connector which periodically executes SQL `SELECT` queries and
emits the resulting rows into Flow as JSON documents. It is designed to
be a flexible but not particularly point-and-click friendly tool, however
if extended with suitable discovery logic it could become much more usable.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-mysql-batch:local -f source-mysql-batch/Dockerfile .
    $ docker exec -it source-mysql-db-1 mysql test --user=root --port=3306 --host=127.0.0.1 --protocol=tcp --password=secret1234
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
              address: "127.0.0.1:3306"
              password: secret1234
              user: root
        bindings: []

And a fleshed-out catalog:

    captures:
      acmeCo/something/source-mysql-batch:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-mysql-batch:local"
            config:
              address: "localhost:3306"
              password: "secret1234"
              user: "flow_capture"
        bindings:
          - resource:
              name: "foobar"
              template: >
                {{if .IsFirstQuery}}
                  SELECT xmin, * FROM public.foobar;
                {{else}}
                  SELECT xmin, * FROM public.foobar
                    WHERE xmin::text::bigint > $1
                    ORDER BY xmin::text::bigint;
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