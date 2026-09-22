# Materialize Iceberg (Rust)

This design is for a new connector, written in Rust primarily for greater
correctness guarantees.  The full connector requirements are quite complex, so
this covers initial work to try to prove that the performance is acceptable.

The connector will do standard updates, but instead of using EMR serverless it
will do all merging in process, this provides better latency, lower costs, and
requires less operational setup.

The connector will have a built-in catalog server implementing the REST catalog
spec.  An implication of this is if the task is not running you can't reach the
catalog server.  In the future we may push to an external catalog.

All data will be persisted in a user provided object storage location.

## Dependencies

Large dependencies/frameworks will be avoided as a principal so that we can
fully leverage performance characteristics of our system.

Some of the major dependencies, to get a feel for what level we want to operate
at:

- tokio
- axum
- parquet
- reqwest
- apache-avro
- object_store

Note: To handle object stores, we could bring in `object_store`, or we could
avoid dependencies with a custom reqwest based library using auth helper crates
but not the full aws/gcp SDKs.

## Testing

Iceberg has a lot of corners, and we intend to write much from scratch.
So it seems important to get an initial list of the readers we will test
against.

Each reader should have an integration test that can be ran.  Perhaps we run
the connector in flowctl to write the data, then run the catalog in a
standalone mode outside of connector-networking.

For CI, testing against DuckDB only may be a good compromise.

## Implementation

Rough ordering of implementation tasks:

1. Setup a null connector in Rust, this is a connector that does nothing but
   can be built and loaded into a local stack, and consume data.  Base this off
   of existing Rust materializations.

2. Get HTTP server running with connector-networking feature, implement
   functionality as needed in later steps.  The connector should show the
   Catalog URL and the `v1/config` endpoint.  Will implement endpoints as
   needed in next steps.

3. Add configuration for an off the shelf catalog for use during development.
   During development it will be useful to compare our catalog API against a
   real catalog API, to check responses and that the files we create are
   acceptable.  This will be a separate binary.  I think iceberg-rust could be
   used for this.

4. Write data with no deletes for one object store, initially S3.  This is the
   simplest write pattern and sets a baseline we can measure for how fast we
   can write the data.

5. Basic loads.  Since the data has no deletions this should be the max
   performance for reads.

6. Add positional deletes to write and fix up loads.

7. Basic compaction and maintenance tasks.

8. Flesh out Catalog REST API.

## Transactions

### Load

Write a temporary parquet file for each chunk of keys, each chunk is sorted.
This could be multiple files or a single file using multipart.

From the current metadata.json, read the manifest list, read that for
the manifests and read that for the data-files.
```
table-metadata.[]snapshots -> manifest-list -> []manifest -> []data-file
```

We can do some pruning based on the key lower and upper bounds when reading the
manifests.  When reading the data-files we can also prune based on the
row-groups stats.

For each file, data or delete, create an iterator that yields (key asc, seq_no
asc), we use a heap queue to yield the values in order, when the key changes
over, merge the records into a single item for the key.

### Store

For each binding.

Write documents to parquet files, one file per N bytes. Configurable size with
10GiB default, same as materialize-s3-parquet.  If the document exists we add
it to a delete file as a position delete (file, row).  Hard deleted documents
are also added to the delete file.

We want to use positional delete because it should be the fastest for most
readers.  For v3 tables, it would be nice to use vectored deletes.

The parquet files are written in ascending key order as this is the order they
are received.

When a data-file or delete-file is flushed, write an avro manifest file.

Record a snapshot-id and the parent snapshot-id in the checkpoint.

Compaction and maintenance tasks could be done in multiple areas.  We don't
want write contention/concurrency, and we don't want to need to redo a lot of
work.  I think we should integrate these tasks into the Store phase based on
metrics like:
- delete-file number of entries and number of files.
- data-file sizes
- manifest file count

Compaction tasks:
- Cleaning up delete files.
- Combine small data-files.
- Combine manifest files.

Maintenance tasks:
- Trim snapshots.
- Cleanup orphan files.

### Acknowledge

On recovery, check the table snapshot for each binding, maybe it is already
applied.

If the parent snapshot-id does not match the one recorded in Store, someone
else has written, a backfill is required.

for each binding:
- Write an avro manifest list.
- Write a new `{}.metadata.json`.

Use the catalog API to commit the snapshot.  Since it is potentially multiple
tables we want to use:

```
POST /v1/{prefix}/transactions/commit
```

Since our Iceberg catalog is backed only on object store, we need to have a
single file that lists all the latest table metadata files.

If this fails with a 409 Conflict, there is another writer, likely need to
backfill.

## Concurrency & Correctness

As in other materializations, a correctness condition is that the connector is
the only writer of the catalog.  Still we must consider the possibility of
multiple connectors running at once.  These could occur due to multiple
connectors being configured to use the same location, system errors or if
the collection has multiple shards.

Iceberg typically uses optimistic locking.  If two connectors attempt the
same operation one should fail and retry the operation.  When losing a race the
connector should exit.

We can use conditional writes on S3, GCS, Azure Blob to make atomic updates.

Example situations:
- Concurrent table creation
- Concurrent table snapshot updates.
- Compaction vs write.
- Multi table transactions.

## Deferred

These are some things we will need, but we can do them after the initial work
is proved.

- Publish to external Catalog.
- Object stores not in initial implementation:
  - GCP
  - Azure
- Vectored deletes for v3 tables.
- Evaluate prune with parquet bloom filter.
- Schema evolution.
- Partition Specs.
- Retention Policy for maintenance tasks.
- Authentication.

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
