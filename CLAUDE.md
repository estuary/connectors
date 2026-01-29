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

The quickest way to build a docker image of a connector is to edit its Dockerfile, comment out the `go test` and `go mod` lines in Dockerfile, and replace `go build` with `COPY ./connector ./`, and use the following set of commands to build the local image:

```bash
cd $connector; GOARCH=amd64 GOOS=linux go build -tags nozstd -v -o ../connector && cd .. && ./build-local.sh $connector
```

## Testing (Go)

- Always provide `TEST_DATABASE=yes UPDATE_SNAPSHOTS=true` when running `go test -v ./$connector`
- For connectors which have a corresponding folder in tests/ or tests/materialize/, also run integration tests using the following command: `CONNECTOR=materialize-mysql VERSION=local ./tests/materialize/run.sh`.
- To run integration tests, must build the docker image locally.

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
