Flow MySQL Source Connector
===========================

This is a flow connector that captures change events from a MySQL database via the
[Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

## Getting Started

Prebuild connector images are available at `ghcr.io/estuary/source-mysql`. See
"Connector Development" for instructions to build locally.

The connector has several prerequisites:
* The [`binlog_format`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format)
  system variable must be set to `ROW` (the default value).
* The capture user must have appropriate permissions:
  - The `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges.
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables is required (if automatic discovery is used).

This can be achieved with the following example commands:

```sql
-- Create the 'flow_capture' user with replication permission and the ability to
-- read all tables. The SELECT permission can be restricted to just the tables that
-- need to be captured, but automatic discovery requires `information_schema` access.
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';

--
SET PERSIST binlog_row_metadata = 'FULL';
```

## Mechanism of Operation

See [`source-postgres/README.md`](https://github.com/estuary/connectors/blob/main/source-postgres/README.md#mechanism-of-operation) for this explanation.

## Connector Development

Any meaningful connector development will require a test database to run
against. To set this up, run:

```bash
$ docker-compose --file source-mysql/docker-compose.yaml up --detach
```

The connector has a reasonably thorough `go test` suite which can be run with:

```bash
$ TEST_DATABASE=yes LOG_LEVEL=debug go test -v ./source-mysql -count=1 -timeout=0
```

For more manual testing or to exercise interaction with the Flow runtime, the
connector can be run through a full discovery and capture with `flowctl`:

```bash
$ cat > acmeCo/flow.yaml <<EOF
captures:
  test/source-mysql:
    endpoint:
      local:
        command:
          - go
          - run
          - ./source-mysql
        config:
          address: "127.0.0.1:3306"
          user: root
          password: secret1234
        protobuf: true
    bindings: []
EOF
$ flowctl raw discover --source acmeCo/flow.yaml
$ flowctl preview --source acmeCo/flow.yaml
```

It's usually not necessary to build a Docker image locally, but you can with:

```bash
$ docker build --network=host -f source-mysql/Dockerfile -t ghcr.io/estuary/source-mysql:local .
```

There's also an integration test which runs automatically as part of a CI build,
and which can be run locally by first building the Docker image and then running:

```bash
$ CONNECTOR=source-mysql VERSION=local ./tests/run.sh
```
