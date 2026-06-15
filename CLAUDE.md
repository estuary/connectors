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
- When running tests, write the full output to a file, and then read the output from the file. Do not `tail` or `head` the test output as it is very large and you are likely not to find the important lines.

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

# PR review

When running the `pr-review-toolkit:review-pr` skill, after the standard review: for every commit that fixes a bug in a specific stream's fetch/parse/cursor logic, check whether the same bug pattern exists in other streams in the same connector. Streams within a connector typically share polling, pagination, and cursor patterns, so single-stream fixes frequently apply to siblings. Report any affected-looking siblings as part of the review output.
