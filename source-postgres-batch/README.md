# PostgreSQL Batch Source Connector

This connector captures data from PostgreSQL databases by periodically
executing SQL queries and emitting results as JSON documents into Flow
collections.

It is meant as an alternative to the CDC connector for situations where
logical replication is unavailable (restrictive managed DBs, read replicas
prior to v16, WAL access restrictions, etc) or unsuitable (capturing from
views or ad-hoc queries).

**For user documentation, see: https://go.estuary.dev/source-postgres-batch**

## Development

### Running Tests

```bash
# Launch test database
docker compose -f ./source-postgres-batch/docker-compose.yaml up -d

# Run basic sanity checks
go test -timeout=10m ./source-postgres-batch -run='TestSimpleCapture'

# Run full test suite (exclude long-running tests)
go test -short -timeout=10m ./source-postgres-batch

# Run with debug logging
env LOG_LEVEL=debug go test -timeout=10m ./source-postgres-batch -run='TestFoobar'

# Regenerate test snapshots
env UPDATE_SNAPSHOTS=1 go test -timeout=10m ./source-postgres-batch
```

### Flowctl Preview

You can run the connector under `flowctl preview` and `flowctl raw discover`
without building a Docker image:

```bash
cat > acmeCo/flow.yaml <<EOF
captures:
  acmeCo/example/source-postgres-batch:
    endpoint:
      local:
        command: ["go", "run", "./source-postgres-batch"]
        config:
          address: "localhost:5432"
          user: "postgres"
          password: "postgres"
          database: "postgres"
        protobuf: true
    bindings: []
EOF

flowctl raw discover --source acmeCo/flow.yaml
flowctl preview --source acmeCo/flow.yaml
```
