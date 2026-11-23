# Oracle materialization integration tests

From the repository root, run:

```bash
UPDATE_SNAPSHOTS=true go test -v ./materialize-oracle/... -timeout 20m
```

Live tests start Oracle Database Free 23ai with Docker Compose and remove the
instance and its volumes when they finish. Docker assigns an available port
on `127.0.0.1`, and the tests use that address for all database connections.
The service is named `db` on the `flow-test` network, which the tests create
if needed. All fixture credentials are made up. Tests require Go, Docker
Compose, and `flowctl` on `PATH`.

To run only the offline tests:

```bash
UPDATE_SNAPSHOTS=true go test -short -v ./materialize-oracle/...
```

To use an existing test database, set `ORACLE_ADDRESS`; this disables Compose
startup and cleanup. Optional overrides are `ORACLE_SERVICE_NAME`,
`ORACLE_USER`, `ORACLE_PASSWORD`, and `ORACLE_SCHEMA`. To manage the local
fixture manually, run from the repository root:

```bash
materialize-oracle/setup.sh
ORACLE_ADDRESS="$(docker compose -f materialize-oracle/docker-compose.yaml port db 1521)" \
  UPDATE_SNAPSHOTS=true go test -v ./materialize-oracle/... -timeout 20m
materialize-oracle/cleanup.sh
```

The tests replace the endpoint configuration in `testdata/config.local.yaml`
with the chosen address and overrides before running each fixture.

The shared SQL suites exercise materialization transactions, schema changes,
and fencing. The migration fixture covers compatible schema changes that
keep the same Oracle column type; it has no `VARCHAR2` columns. Unit tests
cover `VARCHAR2` to `CLOB` migration SQL and resuming an interrupted migration.
The apply suite uses the shared implementation with Oracle's validation.
Its challenging-name case and the materialization suite's
unbounded composite text keys use delta updates because those keys exceed
Oracle's index budget.
The standard-update composite-key cases are in `testdata/oracle.flow.yaml`.

Additional live tests assert which rows survive a source backfill with
`truncate_after_backfill` enabled and disabled, including delta tables and
tables without `flow_published_at`. A binding lifecycle test verifies
checkpoint recovery after reopening, rollback of a fenced commit, the
backfill required to switch to delta updates, and appending the same key in
successive transactions after the switch.

Oracle-specific fixtures retain coverage of source deletes, partial updates,
quoted names, booleans, JSON, timestamps, a 5,000-character CLOB, column types,
and staging-table reuse. They also run with `FLOW_DOCUMENT` excluded and
`/_meta` carrying the document UUID. The shared materialization suite checks
transaction health and, with `FLOW_TEST_RUNTIME=v1` and
`FLOW_TEST_FLOWCTL=flowctl-v1`, runtime v1 liveness and destination rows.

Snapshots live in `.snapshots/`; fencing uses the existing shared SQL
snapshots. New snapshots are created on the first live run. Regenerating an
existing snapshot requires `UPDATE_SNAPSHOTS=true`. Creating or updating a
snapshot fails that run so the change can be reviewed; rerun after reviewing
the diff. Run runtime-next before
runtime v1 so its destination snapshot is available for comparison.

For the repository integration runner, build the connector image with
`./build-local.sh materialize-oracle`, then run:

```bash
CONNECTOR=materialize-oracle VERSION=local ./tests/materialize/run.sh
```

The runner also needs `jq` and checks the built image's spec response.
