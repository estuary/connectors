# InfluxDB materialization tests

Run the repository materialization suite after building the connector image:

```bash
./build-local.sh materialize-influxdb
CONNECTOR=materialize-influxdb VERSION=local ./tests/materialize/run.sh
```

The Go integration test also runs directly with:

```bash
go test -v ./materialize-influxdb/... -run '^TestIntegration$' -count=1
```

The integration test runs on any `go test` without `-short`; use `go test -short` to run only the unit tests. The suite requires Docker Compose and `flowctl`; the unit tests need neither. It uses the shared `flow-test` network and host port 8088, separate from the InfluxDB capture's port 8087.

`setup.sh` recreates the test database from `materialize-influxdb/docker-compose.yaml`, including its organization, bucket, and made-up credentials. `cleanup.sh` removes the database and its volumes. The Go test invokes both scripts, including cleanup after a failed preview.

The previews run the connector as a local command, following other Go materialization tests. The shared runner separately smoke-tests the built image. The first fixture writes two transactions; a second preview starts with fresh runtime state against those existing points and applies updates and source deletions with `flow_document` excluded. It checks that deleting one reading preserves its neighbor one nanosecond later, deleting an order removes both its historical points, and updating an order using the default timestamp creates a second point.

Snapshots in `materialize-influxdb/.snapshots/` compare every returned measurement, tag, field, and InfluxDB field type. Runtime-generated UUIDs in the selected `_meta` JSON strings and publication timestamps of `orders` are normalized. The readback groups points by their actual timestamps first, so missing points or accidentally merged updates still fail. Explicit event timestamps retain their nanosecond precision.

To regenerate snapshots after an intentional behavior change, run the entire integration test with `UPDATE_SNAPSHOTS=true` and review the diff. Failed comparisons write `.actual` files beside the snapshots for CI artifacts. The runtime v1 pass skips this suite because it explicitly exercises `preview-next`.
