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
subcollections nested beneath them (like `users/alice/messages` and `users/bob/messages`), containing
documents such as `users/alice/messages/1` and `users/bob/messages/1`).

The `source-firestore` connector captures all documents with a particular sequence of collection
names, so in the above example the available resources to capture are `users` and `users/*/messages`,
where the latter will contain documents from all users. Captured documents have a property `/_meta/path`
which contains the full path at which the document occurred.

Configuration
-------------

In general you should only need to provide Google Cloud Service Account credentials in JSON
form, and the database name will be inferred from the `project_id` property of those credentials.
However it is possible to explicitly specify the database name if desired.

Capture bindings need to specify a collection path/pattern such as `foo/*/bar/*/baz`, which
will capture all documents with a path like `foo/<fooID>/bar/<barID>/baz/<bazID>`. In addition
they need to specify a `backfillMode` selection. If you're not sure this should probably be set
to `async`.

### Backfill Modes

Each resource binding must specify a `backfillMode`. Allowed values are `none`, `sync`, and `async`:

  - The backfill mode `none` skips preexisting data and just captures new documents,
    and changes to existing documents which occur after the binding was added.
  - The backfill mode `async` streams only new changes (like the `none` mode) but then
    spawns another worker thread in parallel to request chunks of preexisting data, one
    at a time.
    - This mechanism has been tested to work reliably on collections of up to 50 million
      documents and to make progress even when restarted frequently (every 30s, in tests).
    - The downside is that backfilled data and new changes are interleaved without any
      coordination, so any guarantees about duplicated or stale document reads are laxer
      than for `sync` backfills.
    - In addition, incremental progress across restarts works by serializing the path of
      the last document backfilled in the connector state checkpoints, and if the document
      is modified while the connector restarts the backfill will be forced to start over.
      This only happens when the connector restarts, so if the backfill completes in a
      single run of the connector there should be no danger of this happening.
    - Automatic discovery specifies `{"strategy": "maximize", "key": "/_meta/mtime"}`
      for all collections, so in general duplicated or stale reads of documents should
      not alter the materialized outputs from a collection.
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

The `source-firestore` connector has a fairly large suite of automated tests, however a bare
run of `go test` will skip most of them:

    ## Doesn't do much, only runs tests that don't hit the database
    $ go test -v ./source-firestore

This is because the tests need to run against an actual Firestore instance, and the author
doesn't trust the provided [emulator](https://firebase.google.com/docs/emulator-suite/connect_firestore)
to accurately represent the actual behavior of Firestore databases. So you will have to provide
some credentials and set the `TEST_DATABASE` environment variable to `yes` for the tests to do
anything:

    ## Actually runs the automated capture tests
    $ TEST_DATABASE=yes go test -v ./source-firestore --project_id=some-project-123456 --creds_path=~/secrets/some-project-123456-12ab34cd45ef.json

There's also another handfull of "automated tests" in `datasynth_test.go`, which aren't so much
automated tests as they are tools for a human to run to stress-test capture behavior on massive
datasets:

    ## Generates an enormous quantity of synthetic data
    $ DATASYNTH=yes LOG_LEVEL=debug TEST_DATABASE=yes go test -v ./source-firestore -timeout=0 -run TestSyntheticData --project_id=some-project-123456 --creds_path=~/secrets/some-project-123456-12ab34cd45ef.json

    ## Tests a massive backfill
    $ DATASYNTH=yes LOG_LEVEL=debug TEST_DATABASE=yes go test -v ./source-firestore -timeout=0 -run TestMassiveBackfill --project_id=some-project-123456 --creds_path=~/secrets/some-project-123456-12ab34cd45ef.json