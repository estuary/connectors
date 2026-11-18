# Oracle materialization integration tests

From the repository root, run:

```bash
CONNECTOR=materialize-oracle VERSION=local ./tests/materialize/run.sh
```

Build the connector image first with `./build-local.sh materialize-oracle`.
The runner needs Go, Docker Compose, `flowctl`, and `jq`, as do the other Go
materialization tests. `TestMain` runs `setup.sh` and `cleanup.sh` to manage an
independent Oracle Database Free 23ai instance on port 1522. All credentials
in this fixture are made up. The service is named `db` on the `flow-test`
network; setup creates the network if needed. Cleanup removes the instance
and its volumes, including after a failed test.

To run just the connector's tests:

```bash
TEST_DATABASE=yes go test -v ./materialize-oracle/... -timeout 20m
```

Without `TEST_DATABASE=yes` or `CONNECTOR=materialize-oracle`, live tests skip.
Short mode always skips them. To use an existing test database, also set
`ORACLE_ADDRESS`. This disables Compose setup and cleanup. The other optional
overrides are `ORACLE_SERVICE_NAME`, `ORACLE_USER`, `ORACLE_PASSWORD`, and
`ORACLE_SCHEMA`.

The fixtures in `materialize-oracle/testdata/` run through the real Flow runtime
and connector. Snapshots in `materialize-oracle/.snapshots/` compare all rows
after the initial transactions and a second preview with fresh runtime state.
They cover standard updates, delta appends, source deletes, partial updates,
quoted names, booleans, JSON, timestamps, and a 5,000-character CLOB. The same
fixtures run with `FLOW_DOCUMENT` excluded. Additional assertions check
column types, publication timestamps, persisted checkpoints, and fencing.
The collections declare a projection of `/_meta/op`, which the connector
requires to receive source delete markers when `FLOW_DOCUMENT` is excluded.

Set `UPDATE_SNAPSHOTS=true` to regenerate snapshots against a live database
after an intentional change, then review the full diff. For the legacy runtime
pass, also set `FLOW_TEST_RUNTIME=v1` and `FLOW_TEST_FLOWCTL=flowctl-v1`.
