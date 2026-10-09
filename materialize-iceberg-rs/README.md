# Materialize Iceberg (Rust)

This design is for a new connector, written in Rust primarily for greater
correctness guarantees.  The full connector requirements are quite complex, so
this covers initial work to try to prove that the performance is acceptable.

The connector will do standard updates, but instead of using EMR serverless it
will do all merging in process; this provides better latency, lower costs, and
requires less operational setup.

The connector will have a built-in catalog server implementing the REST catalog
spec.  An implication of this is if the task is not running, you can't reach the
catalog server.  In the future we may push to an external catalog.

All data will be persisted in a user-provided object storage location.  This
gives the user full ownership of their data and matches existing connector
design.

## Goals

- Low latency.
- Operational simplicity.
- Iceberg reader compatibility.

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

Each reader should have an integration test that can be run.  Perhaps we run
the connector in flowctl to write the data, then run our catalog in a
standalone mode outside of connector-networking.

For CI, we will test against Trino only.  It has the following advantages:
- depends on many of the same Java libraries used by other products
- open source, so our bugs can be diagnosed more easily
- runs in a single-container without a Spark cluster

For development, a recommended tool is DuckDB due to how lightweight it is.

## Implementation

Rough ordering of implementation tasks:

1. Set up a null connector in Rust, this is a connector that does nothing but
   can be built and loaded into a local stack to consume data.  Base this off
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

5. Implement basic loads.  Since the data has no deletions, this should be the
   max performance for reads.

6. Add positional deletes to write and fix up loads.

7. Basic compaction and maintenance tasks.

8. Flesh out Catalog REST API.

## Transactions

### Connector State

```json
{
    "ParentSnapshotID": "123",
    "Bindings": {
        "{StateKey}": {
             "{RangeKey}": {
                 "SnapshotID": "456",
                 "ManifestEntry": { ... },
                 "DeleteList": "s3://bucket/path/to/deletes-index"
             }
         }
     }
}
```

### Load

Copy the connector state and read the `current-snapshot`, if there is no
pending snapshot or the current snapshot matches, then proceed.  Otherwise
there is an ongoing commit.  In this case we synthesize the snapshot
from the connector state, this must match exactly what Acknowledge will
produce.

Record the `parent-snapshot-id` to be added to the checkpoint in Store.

Iterate over the keys from the runtime, create a chunk and sort them in memory.

Perform a scan of the snapshot.  From the current snapshot, read the manifest
list, read that for the manifests and read that for the data-files.
```python
for manifest_list in current_snapshot:
    for manifest in manifest_list:
        for manifest_entry in manifest:
            yield (seq, manifest_entry.data_file.file_path)
```

Prune based on the key lower and upper bounds when reading the manifests.  When
reading the data-files we can also prune based on the row-groups stats.

Our delete-files only contain positions for a single data-file.  Build a
bitmap/set for each data-file with the delete positions.

Create an iterator for each data-file which contains the bitmap, for every row
it will yield:
```
(key asc, seq_no desc, pos, is_delete)
```

Use a heap queue to yield the values from the underlying iterators in order.
Only the first value for a key or delete marker is needed since our data model
prevents multiple documents with the same key, and documents are complete.
After reading the first key we can advance to the next key.  Keys that aren't
in the chunk of keys can also be skipped.  Deleted items are not emitted and we
add the position to the document for use in Store.

> [!TIP]
> There is a performance opportunity here to sometimes emit extra unrequested
> documents.  This could be used to rewrite small data-files, documents
> returned will be echoed back in the Store stage.

> [!TIP]
> We can read data-files concurrent to building the next chunk of keys.

> [!IMPORTANT]
> We can be sure that the staged files will be added due to the no change
> guarantee in asynchronous tasks, but the delete-files could have changed
> positions.
>
> We could give up on setting delete positions until after Flush,
> but even this doesn't guarantee they will be valid at the next commit.
>
> Depending on how often the snapshot is invalidated we may want to switch to
> writing them as late as possible, or aim to have snapshot invalidations very
> infrequent.

### Store

For each binding.

Write documents to parquet files, one file per N bytes. Configurable size with
256MiB default.  The parquet files are written in ascending key order as this
is the order they are received.

If the document exists, we add it to a delete file as a position delete (file,
row) using the extra data provided in Load.  Hard deleted documents are also
added to the delete file.

Positional deletes are used because they are the most compatible and preferred
by readers.  These are similar to vectored deletes and it should be easy to add
support for these as well.  The downside of positional delete files is they
need to be rewritten if an asynchronous maintenance task writes a new snapshot
ahead of us.

Write a `delete-list` of keys that are added to the delete files, recall that
this includes all updates and hard deletes.  This will be used to rewrite the
positional deletes in case the snapshot is changed during this transaction.

When a data-file or delete-file is uploaded, record a [manifest-entry][] in the
checkpoint.

Record the binding `delete-list`, `snapshot-id` and the `parent-snapshot-id`
in the checkpoint.  The binding `snapshot-id`'s will be combined with other
shards' values to create the actual `snapshot-id` in Acknowledge.

[manifest-entry]: https://iceberg.apache.org/spec/#manifest-entry-fields

### Acknowledge

Only the leader shard will commit a snapshot, other shards return.

Read the current snapshot.

On recovery, check the `current-snapshot-id` for each binding, if it matches
the one in the checkpoint, it has already been applied.

If the snapshot has not been applied and the `parent-snapshot-id` does not
match the one recorded in the checkpoint, another writer has added a new
snapshot.  We need to [rewrite positional deletes][].

Read the `manifest-list` and `manifest` files, we need to know the current
manifests in order to write the new manifest files.

for each binding:
- Write the Avro `manifest` files.
- Write the Avro `manifest-list`.

Use the catalog API to commit the snapshot.  Prepare the
`CommitTransactionRequest` with `add-snapshot` and `set-snapshot-ref` updates
for each table.  Since it is potentially multiple tables we want to use:
```
POST /v1/{prefix}/transactions/commit
```

Of course we are writing the handler for this endpoint too.

Write a `key-index` of key -> (file_path, position).  This can be used to
rewrite the positional deletes from a list of deleted keys.  We could skip
this, but it would mean scanning for the position of all deletes again.  This
index could perhaps be `rocksdb` or `redb` and needs more research, suggestions
welcome.

Since our Iceberg catalog is backed only on object store, we need to have a
single file that lists all the latest table metadata files in order to update
it atomically.

If there is a `409 Conflict` response, we need to [rewrite positional
deletes][] and redo the Acknowledge step.

Bindings that are complete will have their checkpoint cleared and the
`parent-snapshot-id` will be updated.  This is to accommodate the situation
where Acknowledge is run multiple times in a row with a different binding set
to drain the transaction before apply.

#### Rewrite Positional Deletes

This needs to be done if an asynchronous task updated the snapshot.  Since
there is the rule that these tasks cannot add, remove, or modify the documents,
the parquet data-files are still valid and the deletes are the correct set of
keys.  Unfortunately, the positional deletes may need to be rewritten.  If the
data has been altered against the rules, the table may be corrupted.

Open the `delete-list` and read the `file_path` and position for every
document in this list.  Using the `key-index`, look up each key and save the
`(file_path, position)`.  For each `file_path`, write a new positional delete
file.

If we decide not to have a `key-index`, then we would need to scan the
data-files again to find the position.

### Compaction and Maintenance

Compaction and maintenance tasks can be transaction blocking or asynchronous.
Transaction blocking means running as part of the transaction cycle: Load,
Store, Acknowledge and only advancing to the next stage when completing.
Asynchronous is running as a separate loop on its own schedule; this introduces
a concurrent writer.

Transaction blocking has the downside of introducing write latency.  On the
upside, it allows us to guarantee that there will be only one writer.  Iceberg
uses optimistic concurrency control and needing to roll back a transaction may
mean reprocessing the data.

The key insight is that each individual task can use a different method, by
category:

**Transaction Blocking**:

- Remove data-file if all rows are deleted.  When detected this file will be
  marked deleted in the `manifest` which will trigger GC after commit.

- Remove data-file if all rows in a data-file are deleted or about to be
  updated.  This can be detected when there are positional deletes for every
  row.  Mark the file deleted in the manifest.

- Trim snapshots.  As part of commit we can add only latest-N or by time.

- Time based removals.  For delta-updates, we could remove old data/delete
  files from new snapshots.

- Cleanup orphan files.  After successful commit.

  If we walk the expired snapshots, we can produce a list of deleted files and
  remove these.  In pseudocode:
  ```python
  orphaned = []
  for manifest_list in expired_snapshots:
      for manifest in manifest_list:
          if manifest.deleted_files_count == 0:
              continue

          for manifest_entry in manifest:
              if manifest_entry.status != 2:
                  continue
              orphaned.append(manifest_entry.data_file.file_path)
  ```

  This is not atomic with committing the snapshot.  However, we can replay this
  on recovery: if the planned snapshot is in the snapshots we assume we have
  committed, and then we list objects to determine the orphaned snapshots.  In
  the normal case that we just wrote the snapshot we already know the expired
  snapshots.

  An alternative to this method is to scan for files referenced and remove
  everything else, but this would require more listing.  By making this
  blocking, we avoid the case where we remove files that are being staged.

**Asynchronous**:

- Combine small data-files.

  If there are many small writes, the parquet files may be too small.

- Rewrite data-files to remove deleted rows.

  If a file has many deleted rows, it can be made more space efficient by
  rewriting it as a new data-file.

Any of the asynchronous tasks would write a new snapshot, potentially
invalidating delete positions that we have staged.

> [!NOTE]
> We can make one promise that simplifies this system: Asynchronous tasks must
> not change the data.  This disallows us from, for example, doing time/size
> based removals.  What this allows us to do is be sure that we can rewrite a
> commit: because every document still exists and has the same value, we only
> need to rewrite delete positions in order to rebase onto a new snapshot.  We
> never need to re-load the files for a transaction, so we can't get stuck in
> Acknowledge.
>
> If we write delete keys and have a key index, we can quickly generate the
> positional delete files in the Acknowledge step.  Without this, it might take
> a little longer but is still possible.

## Concurrency & Correctness

As in other materializations, a correctness condition is that the connector is
the only writer of the catalog.  Still, we must consider the possibility of
multiple connectors running at once.  These could occur due to multiple
connectors being configured to use the same location, system errors or if
the collection has multiple shards.

Iceberg typically uses optimistic locking.  If two connectors attempt the
same operation, one will fail and we would like it to exit.  However, we can't
tell the difference between a commit conflict due to an asynchronous compaction
task vs a zombie connector.

We can use conditional writes on S3, GCS, Azure Blob to make idempotent updates
to object store.

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
- Data Retention Policy.
- Authentication.
- Shard scale up.
