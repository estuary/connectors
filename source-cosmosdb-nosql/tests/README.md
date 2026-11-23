Run from the repository root with Docker available and `flowctl` on `PATH`:

```sh
uv run --package source-cosmosdb-nosql pytest source-cosmosdb-nosql/tests
```

The session-scoped `cosmosdb` fixture starts and seeds the emulator before the
snapshot tests and tears it down afterward, including when a test fails.
It uses a unique Compose project and a Docker-assigned host port so it can run
alongside other emulators. Seeding and the temporary test catalog use that port.
The snapshot tests run `flowctl raw spec` and `flowctl raw discover` using the
local Python connector and the HTTP emulator. The fixture
contains `shop/orders` with a `/customer` partition key and `shop/events` with an
`/id` partition key. The emulator key is public. The remaining tests exercise
the Azure SDK through an in-memory HTTP transport, including capture and resume.

Manual Compose runs and `test.flow.yaml` still default to `localhost:8091`.

Without the emulator, run the offline tests with
`--ignore=source-cosmosdb-nosql/tests/test_snapshots.py`.
