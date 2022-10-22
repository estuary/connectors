Flow Firestore Source Connector
===============================

This is a Flow capture connector which captures documents and change events from a
[Google Cloud Firestore](https://firebase.google.com/docs/firestore) database.

Data Model
----------

The [Firestore data model](https://firebase.google.com/docs/firestore/data-model) consists of
vaguely JSON-esque documents organized into collections and referenced by ID. Each document can
in turn have [subcollections](https://firebase.google.com/docs/firestore/data-model#subcollections)
associated with it.

As a concrete example, there might be a `users` collection containing documents `users/alice` and
`users/bob`, or possibly `users/1` and `users/2`. Those user documents could in turn have `messages`
subcollections nested beneath them, like `users/alice/messages/123` and `users/bob/messages/456`.

The `source-firestore` connector captures from patterns of nested subcollections across all documents,
so in the above example the available resources to capture are `users` and `users/*/messages`.

Configuration
-------------

TODO(wgd): Describe configuration and prerequisites better, include an example.

The connector requires account credentials and that's pretty much it.

### Backfill Modes

TODO(wgd): Describe backfill modes better.

Each resource binding must specify a `backfillMode`. Allowed values are `none`, `sync`, and `async`:

  - The `none` backfill mode skips preexisting data and just captures new documents
    and changes to existing documents which occur after the binding was added.
  - The `async` backfill mode streams only new changes (like the `none` mode) and then
    spawns another worker thread in parallel to query chunks of data.
    - This mechanism has been tested to work reliably on collections of up to 50 million
      documents and to make progress even when restarted frequently (every 30s, in tests).
    - The downside is that its guarantees about duplicated documents are necessarily laxer
      than `sync` backfills, since backfilled data and new changes are interleaved without
      any coordination. Automatic discovery specifies `{"strategy": "maximize", "key": "/_meta/mtime"}`
      for all collections, so this will generally work out fine in practice.
  - The `sync` backfill mode asks Firestore to stream document changes since the
    beginning of time, and relies on Firestore to make sure we end up with a
    consistent and up-to-date view of the dataset.
    - Unfortunately, Firestore tends to terminate streaming `Listen` RPCs if they
      have not completed that initial sync within about 10 minutes, and only provides
      an updated resume cursor after the initial sync completes.
    - This means that on sufficiently large datasets this backfill mode can fail to ever
      complete, while repeatedly restarting and capturing the same documents over and over.
      It is recommended to only use this mechanism on small datasets or when a human will
      be watching the capture to ensure that it becomes fully caught up within <30m.
  - If you're not sure you should probably use `async`.


Developing
----------

TODO(wgd): Describe how to run the automated tests, and the fact that most tests are gated by environment variables.

    ## Doesn't do much
    $ go test -v ./source-firestore

    ## Actually runs the automated capture tests
    $ RUN_CAPTURES=yes go test -v ./source-firestore --project_id=some-project-123456 --creds_path=~/secrets/some-project-123456-12ab34cd45ef.json

    ## Tests a massive backfill
    $ DATASYNTH=yes LOG_LEVEL=debug RUN_CAPTURES=yes go test -v ./source-firestore -timeout=0 -run TestMassiveBackfill --project_id=some-project-123456 --creds_path=~/secrets/some-project-123456-12ab34cd45ef.json