Flow CockroachDB Source Connector
==================================

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/) which captures
change events from a CockroachDB database using CockroachDB's native **sinkless ("core")
changefeeds**.

CockroachDB speaks the PostgreSQL wire protocol, but it is not PostgreSQL under the hood and has no
logical replication (no replication slots, no `pgoutput`). Its built-in CDC mechanism is the
*changefeed*. A sinkless changefeed — `CREATE CHANGEFEED FOR TABLE ... WITH ...` with no sink URI —
streams change rows back over the same SQL connection, and this connector consumes that stream.

## Connector Development

The connector has a fairly comprehensive suite of automated tests which can be run via `go test`.
Most of them require an external database to interact with, so typical usage looks something like:

```bash
## Start the test database running (with rangefeeds enabled)
$ docker network create flow-test   # once, if it doesn't already exist
$ docker compose -f source-cockroachdb/docker-compose.yaml up -d

## Run the complete test suite against the database we just launched
$ TEST_DATABASE=yes go test -v ./source-cockroachdb/... -count=1
```

The `go test` suite only exercises connector behavior in isolation. There's another integration
test which runs it as part of a Flow catalog as well:

```bash
## Build a local version of the connector image
$ docker build --network=host -t ghcr.io/estuary/source-cockroachdb:local -f source-cockroachdb/Dockerfile .

## Run the integration test
$ CONNECTOR=source-cockroachdb VERSION=local ./tests/run.sh
```

## Connector Usage

Prebuilt connector images are available at
[`ghcr.io/estuary/source-cockroachdb`](ghcr.io/estuary/source-cockroachdb).

The connector requires some amount of database setup in order to run:

* **Rangefeeds must be enabled** (changefeeds depend on them). As an admin user, run once:

  ```sql
  SET CLUSTER SETTING kv.rangefeed.enabled = true;
  ```

  The connector checks this during validation and reports a clear error if it isn't set.

* The capture user needs `SELECT` on the captured tables and the `CHANGEFEED` privilege on them
  (or membership in a role that grants it).

* Changefeeds require a CockroachDB **Enterprise** license for sustained use. As of CockroachDB's
  November 2024 licensing change there is no longer a free "Core" tier; Enterprise is **free for
  organizations under $10M in annual revenue** (telemetry is mandatory) and includes changefeeds. A
  freshly started cluster has a **7-day grace period** during which changefeeds work without a
  license, after which an unlicensed cluster is throttled. This is not a changefeed-specific
  paywall — it's the same license any self-hosted CockroachDB needs to run past the grace period.
  See <https://www.cockroachlabs.com/docs/stable/licensing-faqs>.

## Mechanism of Operation

1. **Discovery** uses CockroachDB's `pg_catalog` to enumerate tables, columns, and primary keys,
   reusing the PostgreSQL type-name conventions CockroachDB exposes (`int8`, `text`, `numeric`,
   `timestamptz`, etc.).
2. **Backfill** reads each table in chunks using `SELECT ... AS OF SYSTEM TIME <hlc>`, fetching
   each row as `to_jsonb(row)`. Chunks read at the latest resolved timestamp the changefeed has
   reported, so a chunk can never publish row state older than an already-emitted change event.
3. **Streaming** runs a changefeed `WITH updated, resolved=..., cursor='<hlc>', no_initial_scan,
   full_table_name`. Every change is emitted (unfiltered), and the changefeed's `resolved`
   checkpoints become the connector's cursor/commit points. On restart the changefeed resumes
   from the persisted cursor with no re-snapshot.

Because the backfill (`to_jsonb`) and the changefeed (`format=json`) produce byte-for-byte identical
JSON encodings of each row, snapshot and streamed documents are consistent without any per-column
decoding in the connector.

CockroachDB changefeeds deliver changes **at-least-once**: a row may be emitted more than once,
especially near the cursor/restart boundary, and the consistent backfill snapshot overlaps the
changefeed's post-snapshot replay. These duplicates collapse automatically: Flow keys each document
by the collection key (derived from the table's primary key) and applies the generated reduction
annotations, so re-delivering a row simply re-applies the same merge and yields the same final
state. Deletes carry `_meta.op: "d"` and the primary key; the generated collection schema reduces
those with `delete: true`, removing the row from materialized destinations.

## Table Support and Caveats

- **Tables without a primary key** are captured using CockroachDB's synthesized hidden `rowid`
  column, which becomes the collection key and appears in captured documents.
- **Hash-sharded primary keys** are supported: the hidden shard column is a deterministic function
  of the visible key columns, so the collection is keyed on the visible columns.
- **FLOAT or DECIMAL primary key columns** cannot be captured: their values are raw JSON numbers,
  which Flow collections cannot be keyed on. Such tables fail validation with a clear error.
- **`REGIONAL BY ROW` tables** (hidden `crdb_region` key column) are not currently supported.
- **Multi-column-family tables** are not supported (changefeeds emit partial per-family rows for
  them); validation rejects them.
- **`TIMESTAMP` and `TIME` (without time zone) columns** are captured as plain strings without
  `date-time`/`time` schema formats, because CockroachDB renders them without a timezone offset.
- **Re-keying a collection onto non-primary-key columns** breaks delete processing: changefeed
  delete events carry only the primary key, so delete documents cannot contain other fields.
