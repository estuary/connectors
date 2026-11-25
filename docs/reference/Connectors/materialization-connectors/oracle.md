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
  - The `PRIVATE_TEMP_TABLE_PREFIX` parameter must keep its default, `ORA$PTT_`.
    Standard updates look up existing documents through private temporary
    tables named `ORA$PTT_FLOW_LOAD_<n>`, which exist only in the connector's
    session.
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
   `SELECT ANY TABLE`, `INSERT ANY TABLE`, `UPDATE ANY TABLE` and
   `DELETE ANY TABLE`. The connector doesn't create schemas.

2. Allow secure connection to Estuary from your hosting environment. Either:

   - Set up an [SSH server for tunneling](/guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint),
     include the additional `networkTunnel` configuration to enable the SSH tunnel.
     SSH Known Hosts is required: set it to the output of `ssh-keyscan` for
     the SSH server, so that the connector verifies the server before sending
     your database traffic through it.
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
| `/schema`                    | Schema        | Schema for bound collection tables and the `flow_checkpoints_v1` table. Defaults to the schema of the user.                         | string  |                  |
| `/sslmode`                   | SSL Mode      | `verify-full` connects over TLS (TCPS) and verifies the server's certificate and host name. `require` connects over TLS without verifying them. `disable` connects over plain TCP. | string | `verify-full` |
| `/hardDelete`                | Hard Delete   | Delete rows from the destination when their documents are deleted in the source. Otherwise `_meta/op` marks deleted documents.      | boolean | `false`          |
| `/networkTunnel`             | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network.                                         | object  |                  |
| `/networkTunnel/sshForwarding` | SSH Forwarding |                                                                                                                                  | object  |                  |
| `/networkTunnel/sshForwarding/sshEndpoint` | SSH Endpoint | Endpoint of the SSH server that supports tunneling, in the form of `ssh://user@hostname[:port]`.                                   | string  |                  |
| `/networkTunnel/sshForwarding/privateKey` | SSH Private Key | Private key to connect to the SSH server.                                                                                        | string  |                  |
| `/networkTunnel/sshForwarding/knownHosts` | SSH Known Hosts | Host keys of the SSH server in OpenSSH known_hosts format, one per line, as printed by `ssh-keyscan -p <port> <host>`. The connector only connects to an SSH server that presents one of these keys. | string | Required with a tunnel |

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
authorities and the host of the address, also through an SSH tunnel. `disable`
connects over plain TCP. The session is then encrypted only if the server
requests Oracle native network encryption, which the connector accepts.

### Changing the schema

The `flow_checkpoints_v1` table records the progress of the materialization in
the endpoint's schema. Changing the endpoint's schema starts it over from a new,
empty `flow_checkpoints_v1` table.

## Table and column names

Oracle stores unquoted names in upper case. A table or field name that is a
valid unquoted identifier and not a reserved word, such as `orderId`, becomes
`ORDERID`, so you can query it without quotes. Any other name, such as
`_meta/op` or `order id`, keeps its exact case and must be quoted, as in
`"_meta/op"`. Oracle names can't contain double quotes, so the connector
replaces them with underscores. Names are limited to 128 bytes, so a field name
with non-ASCII characters can be shorter than 128 characters.

## Data types

| Collection type                                 | Oracle column type                                                   |
| ----------------------------------------------- | -------------------------------------------------------------------- |
| integer                                         | `NUMBER(38,0)`, or `VARCHAR2` when its schema allows more than 38 digits |
| number, or string with `format: number`         | `BINARY_DOUBLE`                                                      |
| boolean                                         | `BOOLEAN`                                                            |
| string                                          | `VARCHAR2(n CHAR)` of its `maxLength`, up to `VARCHAR2(4000 CHAR)`   |
| string with `format: integer`                   | `NUMBER(38,0)`, or `VARCHAR2` when its `maxLength` exceeds 38        |
| string with `format: date`                      | `DATE`                                                               |
| string with `format: date-time`                 | `TIMESTAMP(9) WITH TIME ZONE`, stored in UTC                         |
| string with `format: time`                      | `VARCHAR2(n CHAR)` of its `maxLength`, up to 1000                    |
| string with `contentEncoding: base64`           | `RAW(2000)`                                                          |
| object, array, or several types                 | `VARCHAR2(4000 CHAR)` holding JSON                                   |
| the root document (`flow_document`)             | `VARCHAR2(4000 CHAR)` holding JSON                                   |

Collection keys use types that Oracle allows in a primary key: strings,
including dates, date-times and times, are `VARCHAR2(n CHAR)` of at most 1000
characters, and binary keys are `RAW(2000)`.

Oracle stores an empty string or binary value as `NULL`. String and binary
columns therefore always allow `NULL`, and an empty string or binary value in a
collection key fails the materialization.

The connector doesn't store data in `CLOB` or `BLOB` columns. A value longer than its
`VARCHAR2` column or 4000 bytes, including a document or JSON value, or a binary
value over 2000 bytes, fails the materialization with an error naming the
field.

When a number field changes to a string, its existing values are converted to
text as JSON writes them, so `0.1` stays `0.1`.

Oracle has no year 0, so dates in year 0 are stored as `0001-01-01`. Date-times
are stored in UTC between the years 1 and 9999, so one that falls outside them
in UTC is stored as the first or last moment of that range.

## Transactions

Each transaction of the materialization writes all of its rows in one Oracle
transaction, so the database's undo tablespace must hold a whole transaction.
The first backfill of a large collection, or the removal of the rows published
before a completed backfill, can make for a large transaction.

## Backfills

When a backfill of a collection completes, the rows of a standard table that
were published before the backfill and not rewritten by it are deleted. Without
hard deletes, their `_meta/op` column is set to `d` instead, and the binding
must include the `_meta/op` field for this.

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

## Reserved words

Oracle reserves words such as `DATE`, `NUMBER` and `ORDER`. The connector quotes
names that are reserved words, so they keep their exact case. See the
[Oracle documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Oracle-SQL-Reserved-Words.html)
for the full list.
