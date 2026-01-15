# Estuary Connectors

Capture (`source-*`) and materialization (`materialize-*`) connectors for [Flow](https://github.com/estuary/flow). Connectors are primarily Python (using estuary-cdk) and Go, with some Rust.

## Layout

```
source-*/              # Capture connectors (implement capture.proto)
materialize-*/         # Materialization connectors (implement materialize.proto)
estuary-cdk/           # Python Connector Development Kit
go/                    # Shared Go libraries (auth, network-tunnel, schema-gen)
sqlcapture/            # SQL CDC abstractions for Go captures
materialize-sql/       # SQL materialization base library (Go)
filesource/            # File-based source abstractions (Go)
tests/                 # Integration tests for captures
tests/materialize/     # Integration tests for materializations
```

## Build

### Python Connectors (estuary-cdk)

```bash
./build-local-python.sh source-example
```

### Go Connectors

```bash
cd $connector && GOARCH=amd64 GOOS=linux go build -tags nozstd -v -o ../connector && cd .. && ./build-local.sh $connector
```

## Test

### Unit Tests

```bash
# Go connectors
TEST_DATABASE=yes UPDATE_SNAPSHOTS=true go test -v ./$connector

# Python connectors
cd $connector && poetry run pytest
```

### Integration Tests

Build the local Docker image first, then run:

```bash
# Capture connectors
CONNECTOR=source-postgres VERSION=local ./tests/run.sh

# Materialization connectors
CONNECTOR=materialize-mysql VERSION=local ./tests/materialize/run.sh
```

See [tests/README.md](tests/README.md) for test structure details.

## Development Paths

### Python Connectors (estuary-cdk) — Primary

Most new connectors use the Python CDK. See:
- [estuary-cdk/README.md](estuary-cdk/README.md) — CDK architecture and canonical layout
- [estuary-cdk/CLAUDE.md](estuary-cdk/CLAUDE.md) — Development guidelines

### Go Captures

- [sqlcapture/](sqlcapture/) — SQL CDC (used by source-postgres, source-mysql, etc.)
- [filesource/](filesource/) — File-based sources (S3, GCS, HTTP)
- [source-boilerplate/](source-boilerplate/) — Common capture patterns

### Go Materializations

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

See top-level [README.md](README.md) for transactional semantics and connector patterns (checkpointing, exactly-once delivery).
