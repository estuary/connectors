# InfluxDB materialization tests

Run the repository materialization suite after building the connector image:

```bash
./build-local.sh materialize-influxdb
CONNECTOR=materialize-influxdb VERSION=local ./tests/materialize/run.sh
```

The Go integration tests also run directly with:

```bash
UPDATE_SNAPSHOTS=true go test -v ./materialize-influxdb/... -run '^TestIntegration' -timeout 20m -count=1
```

The integration test runs on any `go test` without `-short`; use `go test -short` to run only the unit tests. The suite requires Docker Compose and `flowctl`; the unit tests need neither. It uses the shared `flow-test` network and host port 8088.

`materialize-influxdb/setup.sh` recreates the test database from `materialize-influxdb/docker-compose.yaml`, including its organization, bucket, and made-up credentials. `materialize-influxdb/cleanup.sh` removes the database and its volumes. The Go test invokes both scripts, including cleanup after a failed preview.

The previews run the connector as a local command, following other Go materialization tests. The shared runner separately smoke-tests the built image. The first fixture writes two transactions; a second preview starts with fresh runtime state against those existing points and applies updates and source deletions with `flow_document` excluded. It checks that deleting one reading preserves its neighbor one nanosecond later, deleting an order removes both its historical points, and updating an order using the default timestamp creates a second point.

Snapshots in `materialize-influxdb/.snapshots/` compare every returned measurement, tag, field, and InfluxDB field type. Runtime-generated UUIDs in the selected `_meta` JSON strings and publication timestamps of `orders` are normalized. The readback groups points by their actual timestamps first, so missing points or accidentally merged updates still fail. Explicit event timestamps retain their nanosecond precision.

To regenerate snapshots after an intentional behavior change, run the entire integration test with `UPDATE_SNAPSHOTS=true` and review the diff. Failed comparisons write `.actual` files beside the snapshots for CI artifacts. The dedicated update and delete test uses `preview-next`; the shared suite runs with both runtimes.

`TestIntegrationShared` runs `bptest.RunMaterializationTest` over the shared `tests/simple` and `tests/data-types` collections and `bptest.RunApplyTest` over the shared validation and Apply cases. A test adapter reads back InfluxDB points and field types and cleans up its measurements; the suites invoke the production driver for validation, Apply, and transactions. The shared transaction test uses soft deletes, while the dedicated fixtures in `materialize-influxdb/testdata/` verify hard deletes. The three mixed-type fields in `tests/data-types` are excluded because InfluxDB rejects field type changes; the existing HTTP error tests continue to assert that such conflicts fail the transaction.

The shared suite checks transaction health and preserves distinct point counts before normalizing publication timestamps. Runtime v1 must produce the same destination data as the runtime-next snapshot. Run the default runtime first to create or verify its snapshot, then select runtime v1 using the pinned `flowctl-v1` binary installed by `fetch-flow.sh`, as CI does:

```bash
FLOW_TEST_RUNTIME=v1 FLOW_TEST_FLOWCTL=flowctl-v1 UPDATE_SNAPSHOTS=true go test -v ./materialize-influxdb/... -run '^TestIntegration' -count=1 -timeout 20m
```

The drain, migration, and key-change suites do not apply: this connector writes during Store, maintains no destination checkpoint or managed schema, and supports only delta updates.
