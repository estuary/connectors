---
description: Capture AlloyDB table updates with Estuary's CDC connector. Setup guide for logical decoding, replication slots, publications, watermarks tables, and SSH tunneling.
slug: /reference/Connectors/capture-connectors/alloydb/
---

# AlloyDB

This connector uses change data capture (CDC) to continuously capture table updates in an AlloyDB database into one or more Estuary collections.

AlloyDB is a fully managed, PostgreSQL-compatible database available in the Google Cloud platform.

This connector is a variant of the [PostgreSQL connector](./PostgreSQL.md).
Refer to that page for additional connector features, usage, and the full
configuration reference. Information specific to AlloyDB and its setup is
presented below.

## Prerequisites

You'll need an AlloyDB database setup with the following:

* Logical decoding enabled
* User role with `REPLICATION` attribute
* A replication slot. This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
    * If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](#configuration).
* A publication. This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.

You'll also need a virtual machine to connect securely to the instance via SSH tunnelling (AlloyDB doesn't support IP allowlisting).

## Setup

To meet the prerequisites, complete these steps.

1. Set [the `alloydb.logical_decoding` flag to `on`](https://cloud.google.com/alloydb/docs/reference/alloydb-flags) to enable logical replication on your AlloyDB instance.

2. In your [psql client](https://cloud.google.com/alloydb/docs/connect-psql), connect to your instance and issue the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.

  ```sql
  CREATE USER flow_capture WITH REPLICATION
  IN ROLE alloydbsuperuser LOGIN PASSWORD 'secret';
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

3. Follow the instructions to create a [virtual machine for SSH tunneling](/guides/connect-network/#setup-for-google-cloud)
in the same Google Cloud project as your instance.

### Network Tunnel

The Network Tunnel section to set up SSH forwarding is required for this connector.
You'll fill in the database address with a localhost IP address,
and specify your VM's IP address as the SSH address.
See [secure connections](/concepts/connectors/#connecting-to-endpoints-on-secure-networks)
for properties and their usage, as well as the [sample config](#sample).

## Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-alloydb:v3"
        config:
          address: "127.0.0.1:5432"
          database: "postgres"
          user: "flow_capture"
          password: "secret"
          networkTunnel:
            sshForwarding:
              sshEndpoint: ssh://sshUser@vm-ip-address
              privateKey: |
              -----BEGIN RSA PRIVATE KEY-----
              MIICX......
              ...
              ...
              -----END RSA PRIVATE KEY-----
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
        target: ${PREFIX}/${COLLECTION_NAME}
```
