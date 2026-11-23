# materialize-cosmosdb-nosql

Materializes whole documents into Azure Cosmos DB for NoSQL containers. See the
[connector documentation](../docs/reference/Connectors/materialization-connectors/azure-cosmosdb-nosql.md)
for configuration and storage behavior.

Run unit tests without a database:

```bash
go test -short ./materialize-cosmosdb-nosql/...
```

The integration suite uses the public Linux Cosmos DB emulator and made-up data.
It starts `docker-compose.yaml`, runs the shared materialization, apply, and
migration suites, and removes its resources and emulator on exit. The shared
fixtures check standard and delta updates, data types, schema changes, and
transaction health with total row-count fidelity. Cosmos-specific previews
also check partial updates across restarts, hard deletes, partition key
validation, and container recreation on backfill.
The challenging-name fixtures partition on their `/_id` key because the emulator
rejects their first key's field name as a partition path. All fields and composite
key components are still materialized.

With Docker, `flowctl`, and the connector image available:

```bash
docker network create flow-test
./build-local.sh materialize-cosmosdb-nosql
CONNECTOR=materialize-cosmosdb-nosql VERSION=local ./tests/materialize/run.sh
```

To run just this connector's integration suite:

```bash
cd materialize-cosmosdb-nosql
go test -v ./... -run '^TestIntegration' -timeout 20m
```

Tests requiring the emulator skip with `go test -short`.
Set `UPDATE_SNAPSHOTS=true` to regenerate snapshots after an intentional change
and review the resulting diff.

The tests run the connector as a local command, so the emulator advertises
`localhost:8093`. For a connector running in Docker, its configured endpoint and
the emulator's `--gateway-endpoint` must instead use a hostname reachable from
that container, with matching gateway and published ports.
