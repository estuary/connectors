# source-firestore

## v3, 2022-10-21

- Added a new required `backfillMode` setting to all resource bindings. Allowed
  values are `none`, `sync`, and `async`:
  - The `none` backfill mode skips preexisting data and just captures new documents
    and changes to existing documents which occur after the binding was added.
  - The `sync` backfill mode asks Firestore to stream document changes since the
    beginning of time, and thus relies on Firestore to make sure we end up with a
    consistent and up-to-date view of the dataset.
    - Unfortunately, Firestore appears to terminate streaming `Listen` RPCs if they
      have not completed that initial sync within at most 10 minutes, and only provides
      an updated resume cursor after the initial sync completes.
    - This means that on sufficiently large datasets this backfill mode can fail to ever
      complete, while repeatedly restarting and capturing the same documents over and over.
      It is recommended to only use this mechanism on small datasets or when a human will
      be watching the capture to ensure that it becomes fully caught up within <30m.
  - The `async` backfill mode streams only new changes (like the `none` mode) and then
    spawns another worker thread in parallel to query chunks of data.
    - This mechanism has been tested to work reliably on collections of up to 50 million
      documents and to make progress even when restarted frequently (every 30s, in tests).
    - The downside is that its guarantees about duplicated documents are necessarily laxer
      than `sync` backfills, since backfilled data and new changes are interleaved without
      any coordination. Automatic discovery specifies `{"strategy": "maximize", "key": "/_meta/mtime"}`
      for all collections, so this will generally work out fine in practice.
  - If you're not sure you should probably use `async`.

## v2, 2022-09-29

- Major overhaul to use Firestore gRPC `Listen` API directly, speed up discovery, and
  tweak the metadata added to captured documents.

## v1, 2022-07-29
- Beginning of changelog.
