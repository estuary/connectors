# Materialize Iceberg (Rust)

A proof-of-concept Flow materialization connector that writes to Apache
Iceberg tables by merging documents in-process, rather than delegating to
Spark on EMR Serverless (as `materialize-iceberg` does) or writing append-only
deltas (as `materialize-s3-iceberg` does).

The connector implements a built-in REST catalog server, so the catalog is
only reachable while the task is running. All table data is persisted to a
user-provided object storage location.

## Status

This is an early-stage scaffold. It implements the Flow materialization
protocol end to end, but does not yet persist any data: `Store` requests are
received and discarded. See the connector's design doc for the planned
implementation order.

## Architecture

Built in Rust, hand-rolling the Flow materialization protocol directly
against the `proto-flow` crate the same way `materialize-kafka` does, since
there is no shared Rust protocol crate in this repo:

- **Protocol Handler** (`lib.rs`): entry point dispatching `Spec`/`Validate`/`Apply`/`Open` requests.
- **Configuration** (`configuration.rs`): endpoint and resource configuration with JSON schema definitions.
- **Validation** (`validate.rs`): field constraint validation.
- **Application** (`apply.rs`): currently a no-op.
- **Transaction Processing** (`transactor.rs`): drives the `Open`/`Flush`/`Store`/`StartCommit`/`Acknowledge` cycle; `Store` is a no-op for now.

## Getting Started

1. **Build**: `cargo build --release`
2. **Test**: `cargo test`
3. **Docker**: `docker build -t materialize-iceberg-rs -f Dockerfile ..`

The connector expects Flow protocol messages on stdin and responds on stdout.
Use `flowctl` for local development and testing.
