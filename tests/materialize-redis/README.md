# Redis materialization tests

Run from the repository root with a built connector image, Docker Compose, Go,
and `flowctl` available:

```bash
CONNECTOR=materialize-redis VERSION=local ./tests/materialize/run.sh
```

The Go integration test calls `setup.sh` to start Redis 7.4 on port 6380 and
registers `cleanup.sh` to remove the container and volumes when it finishes.
The fixture is independent of any existing Redis server. Use an available port
6380; the test clears database 0 of this fixture between cases.

To run just the connector tests, including integration tests:

```bash
TEST_DATABASE=yes go test -v ./materialize-redis/... -timeout 20m
```

Without `TEST_DATABASE=yes` or `CONNECTOR=materialize-redis`, tests that need
Redis skip. `go test -short` also skips them.

Offline unit tests cover custom CA and mutual TLS handshakes, certificate
verification failures, SSH forwarding destinations, Sentinel discovery and
primary changes with separate credentials, RedisJSON reads and writes, TTL
reset/removal, hard deletes, and collision-safe key encoding. Sentinel and
RedisJSON protocol tests use the real Go client against in-memory RESP servers.
Live SSH bastions, managed Redis services, and RedisJSON/Search deployments
still require environment-specific validation. The repository fixture remains
plain Redis and checks compatibility with existing configurations.

The catalog and transaction fixtures are in `materialize-redis/testdata/`.
Each case runs an initial two-transaction preview, an incremental preview, and
a full history replay, each with fresh runtime state. The incremental preview
updates, deletes, and inserts orders while leaving an existing order and event
out of its input. Their keys must be preserved because those collections use
whole-document replacement. Both later previews replay two running totals with
the larger key in one transaction and the smaller pre-existing key in the next,
checking that clearing prefixes for merging reductions prevents double-counting.
Standard and delta bindings run with both hard and soft deletes. A partial
counter update checks that standard updates load and merge the stored document
while delta updates replace it.

The unit tests also model runtime-next by passing saved connector state to Apply
without a previous specification. Runtime-next persists the state returned by
Apply and supplies it to subsequent Apply calls, including initial state from
`flowctl raw preview-next --initial-state`. Matching state must preserve untouched
keys during incremental updates and disable/reactivate cycles, while an increased
backfill still clears the affected prefix.

A separate regression models legacy `flowctl-v1 preview` requests, where
`--initial-state` reaches only Open and cannot prevent Apply from clearing keys.
Previews without matching state supplied to Apply clear standard-update prefixes
with nonzero backfills or schemas that may merge existing values, including
references and unavailable schemas. Those bindings need their full history.
Replacement-only schemas without references preserve keys when the backfill
counter is zero. Preview unit tests run without repository files so their results
do not depend on release-note wording.

Snapshots in `materialize-redis/.snapshots/` compare every Redis key and its full
document. Runtime UUIDs are checked and normalized, and TTLs are checked before
normalizing their countdown. Snapshot mismatches also write `.actual` files for
CI artifacts. Set `UPDATE_SNAPSHOTS=true` to regenerate snapshots after an
intentional behavior change, and review the complete diff.

Integration tests use runtime-next (`flowctl raw preview-next`) by default. CI
also runs these cases with `FLOW_TEST_RUNTIME=v1` and
`FLOW_TEST_FLOWCTL=flowctl-v1`, comparing against the same destination snapshots.
