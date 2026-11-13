---
description: Materialize data collections into Oracle Database with Estuary's connector. Supports Oracle Database 23ai and later, SSH tunneling, TLS, hard deletes and delta updates.
---

# Oracle Database

This connector materializes Estuary collections into tables in an Oracle database.

## Prerequisites

To use this connector, you'll need:

- An Oracle database to which to materialize, and user credentials.
  - Oracle Database 23ai and later are supported, including 26ai.
  - The database character set must be `AL32UTF8`.
  - The connector creates tables in the schema you configure, so the user needs
    to be able to create tables in it and needs a quota on its tablespace.
- At least one Estuary collection

## Setup

1. Connect to the pluggable database as an administrator and create a user for
   the connector. The user's schema holds the materialized tables unless you
   configure a different one.

```sql
CREATE USER flow_materialize IDENTIFIED BY "secret";
GRANT CREATE SESSION, CREATE TABLE TO flow_materialize;
ALTER USER flow_materialize QUOTA UNLIMITED ON USERS;
```

   To materialize into another schema, grant the user privileges on it instead,
   for example `CREATE ANY TABLE`, `ALTER ANY TABLE`, `DROP ANY TABLE`,
   `SELECT ANY TABLE`, `INSERT ANY TABLE`, `UPDATE ANY TABLE`,
   `DELETE ANY TABLE` and `LOCK ANY TABLE`. The connector doesn't create
   schemas.

2. Allow secure connection to Estuary from your hosting environment. Either:

   - Set up an [SSH server for tunneling](/guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint),
     include the additional `networkTunnel` configuration to enable the SSH tunnel.
     See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.

   - [Allowlist the Estuary IP addresses](/reference/allow-ip-addresses) in your firewall rules.

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the below properties to configure an Oracle materialization, which will direct one or more of your Estuary collections to your desired tables in the database.

### Properties

#### Endpoint

| Property                     | Title         | Description                                                                                                                         | Type    | Required/Default |
| ---------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/address`**               | Address       | Host and port of the database. If only the host is specified, port will default to `1521`.                                          | string  | Required         |
| **`/user`**                  | User          | Database user to connect as.                                                                                                        | string  | Required         |
| **`/credentials`**           | Credentials   | Credentials for authentication.                                                                                                     | object  | Required         |
| **`/credentials/auth_type`** | Auth Type     | Method to use for authentication.                                                                                                   | string  | `UserPassword`   |
| **`/credentials/password`**  | Password      | Password for the user.                                                                                                              | string  | Required         |
| **`/database`**              | Database      | Service name of the database (pluggable database) to materialize to.                                                                | string  | Required         |
| `/schema`                    | Schema        | Schema for bound collection tables. Defaults to the schema of the user.                                                             | string  |                  |
| **`/sslmode`**               | SSL Mode      | `verify-full` connects over TLS (TCPS) and verifies the server's certificate and host name. `require` connects over TLS without verifying them. `disable` connects over plain TCP. | string | Required, `verify-full` |
| `/hardDelete`                | Hard Delete   | Delete rows from the destination when their documents are deleted in the source. Otherwise `_meta/op` marks deleted documents.      | boolean | `false`          |

#### Bindings

| Property         | Title              | Description                                                                                                        | Type    | Required/Default |
| ---------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------ | ------- | ---------------- |
| **`/table`**     | Table              | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string  | Required         |
| `/schema`        | Alternative Schema | Schema for this table, instead of the endpoint's.                                                                  | string  |                  |
| `/delta_updates` | Delta Update       | Should updates to this table be done via delta updates.                                                            | boolean | `false`          |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-oracle:v1
        config:
          address: oracle.example.com:1521
          user: flow_materialize
          credentials:
            auth_type: UserPassword
            password: secret
          database: ORCLPDB1
          sslmode: verify-full
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

### TLS

Oracle serves TLS (TCPS) and plain TCP on separate listener ports, commonly
`2484` and `1521`, so choose the SSL Mode that matches the port in the address.
`verify-full` checks the server's certificate against public certificate
authorities and the host of the address, also through an SSH tunnel. Use
`disable` only through an SSH tunnel or with Oracle native network encryption
configured on the server, which the connector accepts when the server requests
it.

## Table and column names

Oracle stores unquoted names in upper case. A table or field name that is a
valid unquoted identifier and not a reserved word, such as `orderId`, becomes
`ORDERID`, so you can query it without quotes. Any other name, such as
`_meta/op` or `order id`, keeps its exact case and must be quoted, as in
`"_meta/op"`. Oracle names can't contain double quotes, so the connector
replaces them with underscores.

## Data types

| Collection type                                 | Oracle column type                                                   |
| ----------------------------------------------- | -------------------------------------------------------------------- |
| integer                                         | `NUMBER(38,0)`, or `VARCHAR2` when its schema allows more than 38 digits |
| number, or string with `format: number`         | `BINARY_DOUBLE`                                                      |
| boolean                                         | `BOOLEAN`                                                            |
| string                                          | `VARCHAR2(n CHAR)` when its `maxLength` is at most 1000, otherwise `CLOB` |
| string with `format: integer`                   | `NUMBER(38,0)`, or `CLOB` when its `maxLength` exceeds 38            |
| string with `format: date`                      | `DATE`                                                               |
| string with `format: date-time`                 | `TIMESTAMP(9) WITH TIME ZONE`, stored in UTC                         |
| string with `contentEncoding: base64`           | `BLOB`                                                               |
| object, array, or several types                 | `CLOB` holding JSON                                                  |
| the root document (`flow_document`)             | `CLOB` holding JSON                                                  |

Collection keys use types that Oracle allows in a primary key: strings,
including dates, date-times and times, are `VARCHAR2(n CHAR)` of at most 1000
characters, and binary keys are `RAW(2000)`.

Oracle stores an empty string as `NULL`. String columns therefore always allow
`NULL`, and an empty string in a collection key fails the materialization. A
string longer than its `VARCHAR2` column also fails it, with an error naming the
field.

Oracle has no year 0, so dates and date-times in year 0 are stored as
`0001-01-01`.

## Transactions

The connector writes each transaction of the materialization in two steps:

1. It stages the transaction's rows in a stage table beside each table, named
   `flow_stage_` followed by the table's name.
2. Once the transaction commits, it applies the staged rows to every table in
   one Oracle transaction, and deletes them from the stage tables.

Queries therefore see each transaction's changes all at once. The database's
undo tablespace must hold the application of a whole transaction. The first
backfill of a large collection, or the deletion of the rows published before a
completed backfill, can make for a large transaction.

The connector drops a stage table with its table, and recreates it when the
table's columns change. It records the transactions it has applied in a
`flow_applied_v1` table in the endpoint's schema. Don't change the rows of
either: a transaction whose staged rows are deleted before they're applied
fails the materialization.

Change the endpoint's schema only while the materialization runs without
errors. A transaction it was applying when it failed is recorded in the old
schema's `flow_applied_v1` table, so the materialization can't resume it from
the new schema.

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

## How rows are updated

With standard updates, the connector updates a row by deleting it and inserting
it again, in the same transaction. As a result:

- Columns of the table that the materialization doesn't write are reset to
  their defaults when the row is updated.
- Foreign keys that reference the table must allow the delete, and an
  `ON DELETE CASCADE` foreign key deletes the referencing rows.
- Triggers and change data capture tools see a delete and an insert rather
  than an update.

## Reserved words

Oracle reserves words such as `DATE`, `NUMBER` and `ORDER`. The connector quotes
names that are reserved words, so they keep their exact case. See the
[Oracle documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Oracle-SQL-Reserved-Words.html)
for the full list.
