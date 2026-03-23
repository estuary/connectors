# materialize-clickhouse

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

To reconcile deletes, we enable automatic background CLEANUP merges.
This means that records marked for deletion are omitted from merge operations; they are dropped.

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

The result of this merge operation is that both `c` records are removed.

**Part 3**

| key | value_a | value_b | flow_published_at | _is_deleted |
|-----|---------|---------|-------------------|-------------|
| a   | alpha   | 10      | T1                | 0           |
| b   | bravo   | 20      | T1                | 0           |
| d   | delta   | 40      | T2                | 0           |

As before, we queried all records in the table (`SELECT * FROM t`), the results would change after merge.
Before merge, `SELECT * FROM t` yields 5 records, including both `c` records.
After merge, the same query yields just 3 records; both `c` versions are gone.

Because Estuary destination tables configure `_is_deleted` in the `ReplacingMergeTree` engine,
the `FINAL` qualifier both deduplicates by `ORDER BY` key and
[skips deleted records](https://clickhouse.com/docs/guides/replacing-merge-tree#querying-replacingmergetree) at query time.
This query yields *3* records both before and after merge:
`SELECT * FROM t FINAL`.
For explicitness, queries can also include a predicate to omit deleted records:
`SELECT * FROM t FINAL WHERE _is_deleted = 0`
