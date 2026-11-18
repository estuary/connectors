---
description: Materialize Estuary collections into Oracle Database tables with standard updates, delta updates, and optional hard deletes.
---

# Oracle Database

This connector materializes Estuary collections into Oracle Database tables.
It creates destination tables, loads existing rows to combine updates, and
writes the resulting rows by collection key. The connector is tested with
Oracle Database Free 23ai.

## Prerequisites

To use this connector, you'll need:

* An Oracle Database instance reachable from your Estuary data plane, normally on port `1521`.
* The database service name, such as `FREEPDB1` for a pluggable database.
* A database character set of `AL32UTF8`, Oracle's default since 12.2, or `UTF8`.
  Other character sets can't store all text, so the connector rejects them.
* A database user and password.
* One or more Estuary collections to materialize.

Use a dedicated database user that owns the destination tables. It needs
`CREATE SESSION`, `CREATE TABLE`, and a quota on its default tablespace.
For example, a database administrator can run the following in the target
pluggable database, substituting a password and an appropriate tablespace
and quota:

```sql
CREATE USER FLOW_MATERIALIZE IDENTIFIED BY "<database-password>"
  DEFAULT TABLESPACE USERS QUOTA 1G ON USERS;
GRANT CREATE SESSION, CREATE TABLE TO FLOW_MATERIALIZE;
```

The connector creates and alters destination tables and creates temporary
staging tables in the connecting user's schema. Each binding uses two global
temporary tables named `FLOW_LOAD_<hash>` and `FLOW_STORE_<hash>`. Their rows
are private to each session, and later sessions reuse the tables, so the
connector doesn't drop them. You can drop them while no materialization is
running, such as after deleting one; tasks recreate the ones they need when
they start. It also manages a
`FLOW_CHECKPOINTS_V1` table in the endpoint's default schema to commit
checkpoints together with the data. Leave this metadata table intact.

If you select another schema, it must already exist. Have your database
administrator grant the connecting user the permissions needed to create,
alter, drop, read, and write tables there, including the checkpoint table.
Oracle schemas belong to database users; the connector does not create users
or schemas.

## Configuration

### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Address | Database host and port, as `host:port`. If the port is omitted, `1521` is used. | string | Required |
| **`/service_name`** | Service Name | Oracle service name to connect to, for example `FREEPDB1`. | string | Required |
| **`/user`** | User | Database user to connect as. | string | Required |
| **`/password`** | Password | Password for the database user. Stored as a secret. | string | Required |
| `/schema` | Database Schema | Default schema for destination tables and `FLOW_CHECKPOINTS_V1`. | string | User's schema |
| `/hardDelete` | Hard Delete | Remove source-deleted rows from tables using standard updates. | boolean | `false` |
| `/advanced/no_flow_document` | Exclude Flow Document | Allow standard updates without storing the root document. | boolean | `false` |
| `/advanced/feature_flags` | Feature Flags | Internal options; change only as directed by Estuary support. | string | Optional |

### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Destination table name. The connector creates the table if needed. | string | Required |
| `/schema` | Alternative Schema | Schema for this binding, overriding the endpoint schema. | string | Endpoint schema |
| `/delta_updates` | Delta Update | Append changes instead of merging them into one row per key. | boolean | `false` |

### Example

```yaml
materializations:
  acme/orders-to-oracle:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-oracle:v1
        config:
          address: oracle.example.com:1521
          service_name: FREEPDB1
          user: FLOW_MATERIALIZE
          password: <database-password>
          hardDelete: true
    bindings:
      - resource:
          table: orders
        source: acme/orders
```

## Updates and deletes

Standard updates maintain one row per collection key. The connector loads
existing values, combines incoming documents according to the collection's
reduction rules, and merges the results into the destination table.

With `hardDelete: true`, source delete events (`_meta.op: "d"`) and collection
delete reductions remove the matching row. With the default `false`, source
deletes are retained as soft deletes; the `_meta.op` value in the stored
document identifies them.

With `delta_updates: true`, each transaction appends its updates, and a table
can contain multiple rows with the same key. Delta tables retain source
delete events as appended records. See [delta updates](/concepts/materialization/#delta-updates).

Standard updates normally require `FLOW_DOCUMENT`, which stores the full
JSON document. To omit it, enable `advanced.no_flow_document` and exclude
the root document in field selection. Loads then reconstruct documents from
the selected columns. See [excluding flow_document with standard updates](/guides/customize-materialization-fields/#excluding-flow_document-with-standard-updates).

With `hardDelete: true`, excluding the root document requires a collection
projection of `/_meta/op`. The connector requires that projection in field
selection so it can recognize source deletes. Ensure the collection schema
declares `_meta.op`, which gives it a projection automatically:

```yaml
properties:
  _meta:
    type: object
    properties:
      op: {type: string}
```

Without a projection of `/_meta/op`, the root document remains required
for standard updates with hard deletes enabled.

The `flow_published_at` field is stored as `FLOW_PUBLISHED_AT`, a timestamp
with time zone recording when Flow published the document. When a source
backfill completes, standard-updates tables delete the rows published before
it, which removes rows that no longer exist at the source. This needs
`flow_published_at` in field selection.

## Identifiers and data types

Simple identifiers use Oracle's uppercase folding: the table `orders`
becomes `ORDERS`, and the field `id` becomes `ID`. A simple identifier starts
with an ASCII letter and contains only letters, digits, `_`, `$`, or `#`.
Reserved words and other identifiers are quoted and retain their spelling,
so `Order Items` becomes `"Order Items"` and `select` becomes `"select"`.
Use matching double quotes when querying these names. Identifiers are
limited to 128 bytes, so non-ASCII field names reach the limit in fewer
characters. Oracle identifiers can't contain a double quote, even when
quoted. Fields with longer names or with a double quote can't be selected;
use a projection with a different name instead. Table and schema names
can't contain a double quote.

| Collection type | Oracle column type |
|---|---|
| Integer | `NUMBER(38)` |
| Number | `BINARY_DOUBLE` |
| Boolean | `NUMBER(1)`, with `1` for true and `0` for false |
| String with `maxLength` of at most 1000 | `VARCHAR2(4000 BYTE)` |
| Unbounded or longer string | `CLOB` |
| String with `format: date-time` | `TIMESTAMP(9) WITH TIME ZONE` |
| String key | `VARCHAR2` of 4 bytes per character of `maxLength`, up to `VARCHAR2(4000)` |
| String key with `format: date-time` | `VARCHAR2(64 BYTE)` holding the RFC 3339 text |
| Object or array | `CLOB` containing JSON |
| Root document (`flow_document`) | `CLOB` containing JSON |

`VARCHAR2` sizes count bytes, and are declared with `BYTE` so they don't
depend on `NLS_LENGTH_SEMANTICS`. A character can take up to 4 bytes, so
strings use `VARCHAR2(4000)` only when `maxLength` is at most 1000. If a
string's `maxLength` later grows past 1000 or is removed, its `VARCHAR2`
column is changed to `CLOB` in place. An existing `CLOB` column is kept if
`maxLength` shrinks.

Oracle can't index CLOB or `TIMESTAMP WITH TIME ZONE` primary key columns, so
key columns use `VARCHAR2`. A string key with a `maxLength` of at most 1000
gets 4 bytes per character, and other string keys get `VARCHAR2(4000)`.
Date-time keys keep their exact RFC 3339 text, so the same instant written
with different UTC offsets is two different keys, as it is in the collection.
Together, the string key columns of a new standard-updates table can use at
most 6000 bytes, to fit Oracle's index size limit. If they would use more,
the publication fails; set `maxLength` on the string key fields or use delta
updates. If a string key's `maxLength` later grows or is removed, so the
existing `VARCHAR2` column is too small, the publication fails until the
binding is backfilled. The backfill recreates the table, so its string keys
must also fit in 6000 bytes.

Oracle stores an empty string as null. Text columns other than keys are
therefore nullable even for required fields, and empty strings in them
read back as null. Collection keys must be non-null and non-empty.

## Limitations

* Tasks use a single shard; splitting a task into multiple shards is unsupported.
* New columns can be added, but changing an existing column's type requires a backfill, except for string columns changing from `VARCHAR2` to `CLOB`.
* A table can have at most 999 columns, one fewer than Oracle's limit, because staging tables add a column.
* A new binding can't use a table that already exists.
* Numbers are stored as `BINARY_DOUBLE`. A nonzero number with a magnitude below about `2.2e-308` (a subnormal double) is stored as `0`.
* A standard-updates table whose string keys need more than 6000 bytes in total is rejected. Two string keys without a `maxLength` need 8000 bytes, so set `maxLength` on them or use delta updates.
* Rows are staged one statement per document, without batching, so large backfills over a slow network are slow.
* Connections are plain TCP. TLS (TCPS listeners, such as Autonomous Database) and SSH tunneling are not supported, so data is encrypted in transit only if the database enforces Oracle native network encryption.
* `NaN`, `Infinity`, and `-Infinity` in strings with `format: number` are stored as null, so their columns are nullable.
* Oracle table changes auto-commit. Data writes and runtime checkpoints are committed together in a separate transaction.
