# source-cockroachdb

A Flow capture connector for [CockroachDB](https://www.cockroachlabs.com/), performing real-time
change data capture (CDC) using CockroachDB's native **sinkless ("core") changefeeds**.

CockroachDB speaks the PostgreSQL wire protocol, but it is not PostgreSQL under the hood and has no
logical replication (no replication slots, no `pgoutput`). Its built-in CDC mechanism is the
*changefeed*. A sinkless changefeed — `CREATE CHANGEFEED FOR TABLE ... WITH ...` with no sink URI —
streams change rows back over the same SQL connection, and this connector consumes that stream.

## How it works

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

### At-least-once delivery and deduplication

CockroachDB changefeeds deliver changes **at-least-once**: a row may be emitted more than once,
especially near the cursor/restart boundary, and the consistent backfill snapshot overlaps the
changefeed's post-snapshot replay. These duplicates collapse automatically: Flow keys each document
by the collection key (derived from the table's primary key) and applies the generated reduction
annotations, so re-delivering a row simply re-applies the same merge and yields the same final
state. Deletes carry `_meta.op: "d"` and the primary key; the generated collection schema reduces
those with `delete: true`, removing the row from materialized destinations.

## Prerequisites

- **Rangefeeds must be enabled** (changefeeds depend on them). As an admin user, run once:

  ```sql
  SET CLUSTER SETTING kv.rangefeed.enabled = true;
  ```

  The connector checks this during validation and reports a clear error if it isn't set.

- The capture user needs `SELECT` on the captured tables and the `CHANGEFEED` privilege on them
  (or membership in a role that grants it).

## Licensing note

Changefeeds require a CockroachDB **Enterprise** license for sustained use. As of CockroachDB's
November 2024 licensing change there is no longer a free "Core" tier; Enterprise is **free for
organizations under $10M in annual revenue** (telemetry is mandatory) and includes changefeeds. A
freshly started cluster has a **7-day grace period** during which changefeeds work without a
license, after which an unlicensed cluster is throttled. This is not a changefeed-specific paywall —
it's the same license any self-hosted CockroachDB needs to run past the grace period. See
<https://www.cockroachlabs.com/docs/stable/licensing-faqs>.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `address` | yes | Host or `host:port` of the database (default port `26257`). |
| `user` | yes | Database user to authenticate as. |
| `password` | yes | Password for the user. Required even against an insecure cluster (which ignores it); supply any value. |
| `database` | | Logical database to capture from (default `defaultdb`). |
| `historyMode` | | Capture change events without reducing them to a final state. |
| `advanced.sslmode` | | Override the `sslmode` connection parameter (`disable`, `require`, `verify-full`, …). |
| `advanced.resolved_interval` | | How frequently the changefeed emits resolved checkpoints (default `1s`). Controls end-to-end latency. The connector also sets `min_checkpoint_frequency` to this value, since CockroachDB otherwise clamps the resolved cadence to 30s. |
| `advanced.backfill_chunk_size` | | Rows fetched per backfill query (default `250000`; deliberately larger than other connectors because each chunk is followed by a changefeed fence costing several seconds). |
| `advanced.skip_backfills` | | Comma-separated fully-qualified tables to not backfill. |
| `advanced.discover_schemas` | | Restrict discovery to specific schemas. |

## Table support and caveats

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

## Tests

```bash
docker network create flow-test   # once, if it doesn't already exist
docker compose up -d              # starts CockroachDB with rangefeeds enabled
TEST_DATABASE=yes go test ./source-cockroachdb/...
```
