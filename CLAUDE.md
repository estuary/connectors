# Estuary Connectors

Capture (`source-*`) and materialization (`materialize-*`) connectors for [Flow](https://github.com/estuary/flow). Connectors are primarily built in Python and Go, with a few in Rust.

## Layout

```
source-*/              # Capture connectors (implement capture.proto)
materialize-*/         # Materialization connectors (implement materialize.proto)
estuary-cdk/           # Python Connector Development Kit
go/                    # Shared Go libraries (auth, network-tunnel, schema-gen)
sqlcapture/            # SQL CDC abstractions for Go captures
materialize-sql/       # SQL materialization base library (Go)
filesource/            # File-based source abstractions (Go)
tests/                 # Integration tests for captures and materializations (Go and Rust)
```

## Build Docker image locally

### Python Connectors (estuary-cdk)

```bash
./build-local.sh source-example
```

### Go Connectors

The quickest way to build a docker image of a connector is to edit its Dockerfile, comment out the `go test` and `go mod` lines in Dockerfile, in their place add `COPY vendor ./vendor`, and add `-mod=vendor` to `go build` and use the following set of commands to build the local image:

```bash
go mod vendor
./build-local.sh $connector
```

This is primarily a Go monorepo with YAML configs. When editing Go code, always run `go build ./...` and `go vet ./...` in the affected module after changes to catch import cycles, missing arguments, and unused imports before declaring a fix complete.

## Testing (Go)

- Always provide `UPDATE_SNAPSHOTS=true` when running `go test -v ./$connector`
- Integration tests are run as part of `go test -v ./$connector`. Feel free to always run the integration tests for both local and cloud connectors. The credentials are provided as part of the integration tests automatically so you do not need to manually read the credentials.
- In order to test feature flags, you can set feature flags for sops-encrypted cloud credentials using `sops set` without reading the encrypted files.
- When running integration tests, run the whole suite (`TestIntegration/*`), since running only one of them can lead to inconsistent snapshots at the moment.

See [tests/README.md](tests/README.md) for test structure details.

## Development Paths

### Python Connectors

- [estuary-cdk/README.md](estuary-cdk/README.md) — CDK architecture and canonical layout
- [estuary-cdk/CLAUDE.md](estuary-cdk/CLAUDE.md) — Development guidelines

### Go Captures

- [sqlcapture/](sqlcapture/) — SQL CDC (used by source-postgres, source-mysql, etc.)
- [filesource/](filesource/) — File-based sources (S3, GCS, HTTP)
- [source-boilerplate/](source-boilerplate/) — Common capture patterns

### Go Materializations

For materialization connector work: each database (Snowflake, Spanner, BigQuery, ClickHouse, Redshift, etc.) has unique DDL limitations. Before implementing a fix, check the specific database's constraints (e.g., Spanner doesn't support RENAME COLUMN, Snowflake doesn't support nullable keys at DDL layer, Redshift encodes VARBYTE as hex).

- [materialize-sql/](materialize-sql/) — SQL destination base library
- [docs/materialize/README.md](docs/materialize/README.md) — Materialization patterns

## Guidelines

### Code Style

- Write comments that document design rationale, broader context, or non-obvious behavior
- Do NOT write comments that describe obvious behavior (e.g., `// Get credentials` before `getCredentials()`)
- Favor functional programming over procedural
- Use early returns over nested conditionals

### JSON Schema

All connector configurations use JSON Schema. See [config_schema_guidelines.md](config_schema_guidelines.md) for field annotations (`title`, `description`, `secret`, `advanced`, `discriminator`).

### Data Types

- Date-time values with `format: date-time` must be RFC3339 compliant
- Prefer schema inference over overly prescriptive schemas

### Testing

- Prefer snapshot tests over fine-grained assertions
- Include edge cases: data types, limits, error conditions
- Integration tests capture to SQLite and verify against `expected.txt`

## Protocol References

- [capture.proto](https://github.com/estuary/flow/blob/master/go/protocols/capture/capture.proto) — Capture protocol
- [materialize.proto](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto) — Materialization protocol

See top-level [README.md](README.md) for transactional semantics and connector patterns.

# Debugging
 
When debugging CI test failures, always ask the user for complete CI logs before exploring the codebase extensively. Do not theorize about root causes from incomplete output — request the full logs first.

# Insights

When making changes that affect function signatures or shared interfaces, immediately grep for all callers and update them before running tests. Use `grep -rn 'FunctionName' --include='*.go'` to find all references.

# Work in progress: `jgm-ice-ns` — Iceberg nanosecond timestamps

This branch implements GitHub issue [estuary/connectors#4462](https://github.com/estuary/connectors/issues/4462) — a task-level option for `materialize-s3-iceberg` to use Iceberg `timestamptz_ns` columns instead of `timestamptz`. Plan file: `~/.claude/plans/implement-the-feature-for-wise-cat.md`.

## Status: blocked upstream

`pyiceberg` 0.10.0 and 0.11.1 (latest) **cannot write** to Iceberg v3 tables, which is required for `timestamptz_ns`. Two distinct failures surface during `txn.add_files(...)`:

1. `pyiceberg.manifest.write_manifest_list` only implements V1/V2 writers — v3 raises `Cannot write manifest list for table version: 3`. See `pyiceberg/manifest.py` `write_manifest_list()` near the end of the file; there's no `ManifestListWriterV3` class.
2. `data_file_statistics_from_parquet_metadata` calls `pyarrow.lib.TimestampScalar.as_py` on parquet column min/max stats, which refuses ns→us downcast: `Nanosecond resolution temporal type ... is not safely convertible to microseconds`.

Both are in pyiceberg core, not in `iceberg-ctl`. Until pyiceberg ships v3 write support (manifest list V3 + nanosecond-aware stats collection), users cannot append data to a `timestamptz_ns` table from Python.

## What's done on this branch

Two commits:
- `e342df74f` — `go/writer`: added `LogicalTypeTimestampNanos`, `WithParquetTimestampAsNanoseconds()`, `getTimestampNanosVal`, and tests (verified passing).
- HEAD — `materialize-s3-iceberg`: full scaffolding for the `nanosecond_timestamps` option.

Files changed in HEAD:
- `materialize-s3-iceberg/iceberg-ctl/pyproject.toml` + `poetry.lock` — pyiceberg `^0.7.0` → `^0.10.0`.
- `materialize-s3-iceberg/iceberg-ctl/iceberg_ctl/models.py` — Pydantic `AdvancedConfig.nanosecond_timestamps`.
- `materialize-s3-iceberg/iceberg-ctl/iceberg_ctl/__main__.py` — imports `TimestamptzNanoType`; `_field_to_type` handles `"timestamptz_ns"`; `IcebergColumn.type` Literal extended; `create_table` sets `format-version=3` when any column is `timestamptz_ns`.
- `materialize-s3-iceberg/driver.go` — `advancedConfig.NanosecondTimestamps`; `MapType` wires `clampTimestampNanos` for `icebergTypeTimestamptzNs`; threads flag into `parquetSchema()` / `projectionToParquetSchemaElement()`.
- `materialize-s3-iceberg/type_mapping.go` — `icebergTypeTimestamptzNs`; `buildSchemaOptions(nanoTs)` replaces the package-level `schemaOptions` var; `parquetTypeToIcebergType` handles `LogicalTypeTimestampNanos`.
- `materialize-s3-iceberg/clamp.go` — `clampTimestampNanos` clamps to int64-ns bounds (`time.Unix(0, math.MinInt64).UTC()` / `MaxInt64`), formatted via RFC3339Nano.
- `materialize-s3-iceberg/catalog.go` — `CreateResource` / `UpdateResource` pass `c.cfg.Advanced.NanosecondTimestamps` through.
- `materialize-s3-iceberg/clamp_test.go` — `TestClampTimestampNanos` (7 cases, all passing).
- `materialize-s3-iceberg/driver_test.go` — `runTimestampNsRegression` subtest of `TestIntegration` (**fails** at `add_files` due to the upstream blocker above).
- `materialize-s3-iceberg/.snapshots/TestSpec` — regenerated; diff is exactly the new property.

## Verification done

- `go build ./...` and `go vet ./...` clean from `materialize-s3-iceberg/`.
- Unit tests: `TestClampTimestamp`, `TestClampTimestampNanos`, `TestClampDate`, `TestSpec` all pass.
- Integration: `TestIntegration/{materialize,apply,ts-overflow-regression,date-overflow-regression}` still pass (no regression to existing flag-off paths). `TestIntegration/ts-ns-regression` fails as described above.

## How to pick up on another workstation

1. `cd connectors-zeta/materialize-s3-iceberg/iceberg-ctl && poetry install` to materialize the upgraded venv. `TestMain` in `driver_test.go` resolves `PYTHON_PATH` from the poetry venv automatically.
2. Decide the path forward with the user. Options were enumerated at the end of the last session — paraphrased:
   - **A.** Revert the integration test only and ship the Go/Python scaffolding behind the flag (risk: users could enable a non-working flag).
   - **B.** Revert everything; wait for pyiceberg v3 write support.
   - **C.** Add a `Validate()` error when `nanosecond_timestamps=true` is set, so the flag is wired but explicitly refused at apply time; flip the error to a no-op once pyiceberg ships v3 writes.
   - **D.** Reimplement an alternate append path (out of scope).
3. Track upstream: watch the pyiceberg repo for `ManifestListWriterV3` and nanosecond-aware `data_file_statistics_from_parquet_metadata`. Until both land, this feature cannot work end-to-end via the current `iceberg-ctl` `add_files` path.
4. If the user picks option **C**, the natural place for the guard is `config.Validate()` in `materialize-s3-iceberg/driver.go:104`, with a message that links to this CLAUDE.md section.
