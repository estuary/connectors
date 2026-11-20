# materialize-cosmosdb-nosql

Materializes whole documents into Azure Cosmos DB for NoSQL containers. See the
[connector documentation](../docs/reference/Connectors/materialization-connectors/azure-cosmosdb-nosql.md)
for configuration and storage behavior.

Run unit tests without a database:

```bash
go test ./materialize-cosmosdb-nosql/...
```

The integration suite uses the public Linux Cosmos DB emulator and made-up data.
It starts `docker-compose.yaml`, exercises two previews against the same
containers, checks snapshots, and removes its resources and emulator on exit.
It covers standard and delta updates, hard deletes, partition key validation,
and container recreation on backfill.

With Docker, `flowctl`, and the connector image available:

```bash
docker network create flow-test
./build-local.sh materialize-cosmosdb-nosql
CONNECTOR=materialize-cosmosdb-nosql VERSION=local ./tests/materialize/run.sh
```

To run just this connector's integration suite:

```bash
cd materialize-cosmosdb-nosql
TEST_DATABASE=yes go test -v ./... -run '^TestIntegration' -timeout 20m
```

Tests requiring the emulator skip unless `TEST_DATABASE=yes` or
`CONNECTOR=materialize-cosmosdb-nosql` is set, and also skip with `go test -short`.
Set `UPDATE_SNAPSHOTS=true` to regenerate snapshots after an intentional change
and review the resulting diff.

The tests run the connector as a local command, so the emulator advertises
`localhost:8092`. For a connector running in Docker, its configured endpoint and
the emulator's `--gateway-endpoint` must instead use a hostname reachable from
that container, with matching gateway and published ports.
