# materialize-testctl

Reads a materialized resource back, drops one, or sweeps away what earlier test
runs left behind — from outside the connector.

This exists for flow's `materialize-consistency` suite, which runs a
materialization against a real Flow runtime and then has to check what actually
landed in the destination. Neither capability belongs in the materialization
protocol: it has no request that reads a destination back, and removing a
binding deliberately leaves its table in place, since destroying a user's data
as a side effect of a catalog edit would be indefensible. A test harness has the
opposite problem, because it names resources per-run and otherwise accumulates
them forever in a shared warehouse.

So both are reached the same way the connector's own integration tests reach
them: `Materializer.SnapshotTestResource` and `Materializer.DeleteResource`, via
the helpers in [materialize-boilerplate/testutil](../../../materialize-boilerplate/testutil/resources.go).
This program only supplies the out-of-process entry point.

## Usage

```console
go run ./tests/materialize/testctl \
    -connector materialize-databricks \
    -config /path/to/endpoint-config.yaml \
    -resource /path/to/resource-config.yaml \
    -mode snapshot
```

Configurations are file paths rather than inline values, because an endpoint
configuration carries credentials and an argument vector is readable by anyone
who can list processes. Either file may be JSON or YAML, and is parsed exactly
as the connector parses what the runtime hands it, so a configuration this
accepts is one the connector accepts.

### Modes

- `snapshot` writes the resource's rows to stdout as newline-delimited JSON, one
  object per row keyed by column name. Rows are objects of columns rather than
  the collection documents that produced them, since that is what a materialized
  resource holds — a standard binding need not carry a root document at all. The
  order is whatever the destination returns, so a caller needing a stable
  ordering must sort.
- `drop` removes the resource named by `-resource`.
- `sweep` removes every test resource in that resource's namespace, and the task
  metadata of test materializations, that an earlier run left behind. Anything
  recent enough that a run still in flight could be using it is left alone. Pass
  `-dry-run` to see what it would remove, and `-log-level debug` to also see what
  it would leave and why.

`sweep` is the mode worth reaching for after a suite has been developed against a
shared warehouse for a while. Dropping by name can only remove resources the
caller knows it created, so the leftovers of runs that crashed or were cancelled
accumulate; sweeping enumerates what is actually present.

A resource is recognised as a test resource by its name: sweeping selects those
containing `_flow_test_` followed by the run's unix timestamp, which is the
convention this repository's integration tests follow. The timestamp is what
allows a sweep to leave a concurrent run's resources alone, so a harness that
wants to be cleaned up after should name its resources the same way —
`<name>_flow_test_<unix>`. Resources named any other way are enumerated but
never touched.

## Supported connectors

- `materialize-azure-fabric-warehouse`
- `materialize-bigquery`
- `materialize-bigtable`
- `materialize-clickhouse`
- `materialize-databricks`
- `materialize-dynamodb`
- `materialize-elasticsearch`
- `materialize-iceberg`
- `materialize-mongodb`
- `materialize-motherduck`
- `materialize-mysql`
- `materialize-postgres`
- `materialize-redshift`
- `materialize-s3-iceberg`
- `materialize-snowflake`
- `materialize-spanner`
- `materialize-sqlserver`

A connector can be driven once two things are true. Its package must be
importable — `package connector` with `func main` under `cmd/connector` — and it
must expose the Materializer: either `NewDriver`, whose `*sql.Driver` carries
`NewMaterializer`, or a package-level `NewMaterializer`. Then add it to the
`connectors` map in `main.go`.

Connectors materializing into something that cannot be read back are left out
even though they are importable: `materialize-eventbridge` and `materialize-sns`
publish to an event bus, `materialize-hubspot` writes through an API, and
`materialize-s3-parquet` writes files rather than a queryable store. There is
nothing for a snapshot or a sweep to enumerate in any of them.
