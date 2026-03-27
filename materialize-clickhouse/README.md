# materialize-clickhouse

**Minimum ClickHouse version: 25.3** — required for the `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge` table setting used by automatic background CLEANUP merges. ClickHouse Cloud users should verify their cluster version meets this requirement.

## ClickHouse Tables

Tables don't have primary keys in the usual sense.
More importantly, they have an `ORDER BY` key, which is a list of columns named when the table is created.
(There is a `PRIMARY KEY` feature, but we'll ignore it for our purposes.)

Tables are physically composed of parts.
A part is a subset of records in the table, stored as a directory of immutable files.
Records are stored in `ORDER BY` key sort order within each part.

Queries perform best when:

1) they use the `ORDER BY` key in filter predicates
2) the table contains fewer parts
3) the table parts do not have overlapping `ORDER BY` key ranges

The ClickHouse [part merge task](https://clickhouse.com/docs/merges) runs periodically to optimize parts.
It identifies small parts, and parts with overlapping `ORDER BY` key ranges.
These parts are merged to improve storage and query efficiency.

## Writing to ClickHouse

When a client issues an INSERT request, ClickHouse creates a new table part for the new records.
Therefore, batching is critical; they recommend batches of 10,000 to 100,000 records.

ClickHouse tables are append-only; clients can semantically INSERT, but not UPDATE or DELETE.
In the Estuary data model, updating and deleting records are core features.

To reconcile updates, we utilize the ClickHouse ReplacingMergeTree engine.
ReplacingMergeTree augments the merge process by dropping duplicate rows (according to the `ORDER BY` key),
retaining only the most recent version.

To reconcile deletes, we configure `_is_deleted` as a deletion marker in the ReplacingMergeTree engine.
During regular merges, the tombstone record (`_is_deleted = 1`) is retained as the winning version.
The `FINAL` query qualifier excludes these tombstones at query time.
We also enable automatic background [CLEANUP merges](#cleanup-merge-settings),
which periodically remove tombstones from disk entirely.

## Querying ClickHouse

Between the time that duplicate records are (a) inserted and (b) merged, queries yield all versions of every record,
including versions marked for deletion.
Mitigations are discussed [later in this document](#Illustrated).

## Managing Estuary Records

The Estuary key fields are the ClickHouse `ORDER BY` columns.

Estuary destination tables add a special column for merge operations: `_is_deleted UInt8`.
The ReplacingMergeTree engine is configured with `flow_published_at` as the
[version column](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree#ver)
and `_is_deleted` as the deletion marker.

To delete a record, we insert a record with `_meta/op = 'd'`.
The `_is_deleted` column is automatically inferred by ClickHouse via a MATERIALIZED expression: `if(_meta/op = 'd', 1, 0)`.

To update a record, we clone the previous version of the record, update the fields so specified, and insert the clone.
`flow_published_at` is a standard Estuary metadata timestamp that naturally increases with each transaction,
so the row with the highest timestamp is retained at merge time.

## CLEANUP Merge Settings

Target tables are created with four SETTINGS that enable automatic background CLEANUP merges:

| Setting | Value | Purpose | Since |
|---------|-------|---------|-------|
| `allow_experimental_replacing_merge_with_cleanup` | `1` | Enables the CLEANUP merge feature (experimental). Required for the other settings to have any effect. Also enables manual `OPTIMIZE TABLE ... FINAL CLEANUP`. | 23.12 / 24.1 |
| `min_age_to_force_merge_seconds` | `604800` (1 week) | Minimum age of all parts in a partition before forcing a merge. Only partitions where every part is older than this threshold are eligible. | 22.10 |
| `min_age_to_force_merge_on_partition_only` | `1` | Restricts forced merges to only run when merging an entire partition into one part. Required for CLEANUP to safely remove deleted rows (ensures no older versions remain in other parts). | 22.10 |
| `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge` | `1` | Enables automatic background CLEANUP merges when the age threshold is met. Without this, cleanup only happens via manual `OPTIMIZE ... FINAL CLEANUP`. | 25.3 |

Together, these settings mean that once all parts in a partition are older than one week,
ClickHouse will automatically perform a CLEANUP merge that physically removes rows where `_is_deleted = 1`.

## Connector Transaction Architecture

The connector writes to ClickHouse using a three-phase transaction: **load**, **store**, **acknowledge**.
Two auxiliary table types support this: load tables and store tables.

### Load Phase

Load tables use the ClickHouse [Join engine](https://clickhouse.com/docs/engines/table-engines/special/join),
an in-memory hash table keyed by the binding's `ORDER BY` keys.
They are named `flow_temp_load_{binding}_{rangeKey}_{table}`.

During the load phase, the connector inserts the keys of documents it needs to look up
into the load table, then joins the target table (using `FINAL` for deduplication) against
the load table to retrieve the current version of documents for those keys.

- **Created** with `CREATE OR REPLACE TABLE` on first use in each load phase
- **Dropped** after each load phase completes (frees server memory)
- **Dropped** on connector shutdown (safety net for any remaining tables)

### Store Phase

Store tables stage documents written during a transaction's store phase.
They are named `flow_temp_store_{binding}_{rangeKey}_{table}`.

Store tables are created with `CREATE OR REPLACE TABLE ... AS {target}`, which clones the target table's
schema and engine (ReplacingMergeTree). This makes their on-disk parts partition-compatible
with the target table, which is critical for the acknowledge phase.

- **Created** with `CREATE OR REPLACE TABLE` on first document in each store phase
- **Dropped** after each acknowledge phase (once partitions are moved to target)

### Acknowledge Phase

On acknowledge, the connector enumerates the store table's parts via `system.parts`
and moves each partition to the target table using `ALTER TABLE ... MOVE PARTITION`.

Moving a partition is a **metadata-only operation**: it relinks existing parts in the filesystem
rather than copying or rewriting data. This makes the acknowledge phase very low latency regardless
of transaction size, and is significantly faster than `INSERT ... SELECT` or other bulk-copy methods.

The acknowledge is **not atomic** across bindings: partitions are moved one at a time. A failure
mid-acknowledge leaves some partitions moved and others not. This is safe because the target table
uses ReplacingMergeTree — re-applying a partial acknowledge is idempotent, since duplicate versions
are deduplicated by `ORDER BY` key and `flow_published_at`.

## Illustrated

### Simple Merge

Two batches have been Store()'d in the table, resulting in two parts.

**Part 1** (batch at T1)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| d   | delta   | 40      | T1                | 0           |

**Part 2** (batch at T2)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| b   | bravo   | 20      | T2                | 0           |
| c   | charlie | 30      | T2                | 0           |

The merge operation consolidates Parts 1 and 2 into Part 3.

**Part 3**

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T2                | 0           |
| c   | charlie | 30      | T2                | 0           |
| d   | delta   | 40      | T1                | 0           |

If we queried all records in the table (`SELECT * FROM t`), the results would be identical before and after the merge.

### Merge after Update

Again, two batches have been Store()'d, but this time there are duplicate `b` records.
The `b` record in Part 2 has a new `value_a` value and a later `flow_published_at`.

**Part 1** (batch at T1)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T1                | 0           |

**Part 2** (batch at T2)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| b   | BRAVO!  | 20      | T2                | 0           |
| c   | charlie | 30      | T2                | 0           |

The merge operation replaces Parts 1 and 2 with Part 3.
This time that `b` record in Part 1 is removed, and that in Part 2 is retained (it has the higher `flow_published_at`).

**Part 3**

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | BRAVO!  | 20      | T2                | 0           |
| c   | charlie | 30      | T2                | 0           |

If we queried all records in the table (`SELECT * FROM t`), the results would change after merge.
Before merge, `SELECT * FROM t` yields 4 records, including both `b` records.
After merge, the same query yields just 3 records; the old `b` version is now gone.

We can't determine when merge operations occur, but we can improve our SQL queries to mitigate.
Adding the `FINAL` SQL qualifier, this query yields just three records *before and after merge*:
`SELECT * FROM t FINAL`.

### Merge after Delete

In this case, two batches have been Store()'d, and there are duplicate `c` records.
The `c` record in Part 2 has `_is_deleted = 1`, instead of `0`.

**Part 1** (batch at T1)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T1                | 0           |
| c   | charlie | 30      | T1                | 0           |

**Part 2** (batch at T2)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| c   | charlie | 30      | T2                | 1           |
| d   | delta   | 40      | T2                | 0           |

A **regular merge** deduplicates the `c` records, retaining the one with the highest `flow_published_at`.
The tombstone (`_is_deleted = 1`) survives as the winning version:

**Part 3** (after regular merge)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T1                | 0           |
| c   | charlie | 30      | T2                | 1           |
| d   | delta   | 40      | T2                | 0           |

`SELECT * FROM t` yields 5 records before merge (both `c` versions) and 4 records after (one `c` tombstone remains).

A **CLEANUP merge** goes further: it physically removes records where `_is_deleted = 1`.
This is the eventual steady state once parts are old enough (see [CLEANUP Merge Settings](#cleanup-merge-settings)):

**Part 3** (after CLEANUP merge)

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T1                | 0           |
| d   | delta   | 40      | T2                | 0           |

`SELECT * FROM t` now yields 3 records.

Regardless of merge state, the `FINAL` qualifier handles both cases at query time.
Because Estuary destination tables configure `_is_deleted` in the `ReplacingMergeTree` engine,
`FINAL` both deduplicates by `ORDER BY` key and
[skips deleted records](https://clickhouse.com/docs/guides/replacing-merge-tree#querying-replacingmergetree).
This query yields *3* records at all times — before merge, after regular merge, and after CLEANUP merge:
`SELECT * FROM t FINAL`.
For explicitness, queries can also include a predicate to omit deleted records:
`SELECT * FROM t FINAL WHERE _is_deleted = 0`
