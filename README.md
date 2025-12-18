# Estuary Connectors

This repo hosts development for connectors for use with Flow.

The `source-*` connectors all implement the [flow capture
protocol](https://github.com/estuary/flow/blob/master/go/protocols/capture/capture.proto). Source connectors in this repo can be used with Flow.

The `materialize-*` connectors all implement the [Flow Materialize gRPC protocol](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto), and only work with Flow.

All connectors in this repository are dual licensed under MIT or Apache 2.0 at your discretion.

# Developing a New Connector

There are some things to consider and some tips here for developing a new connector.

- [ ] Writing integration tests is highly recommended, it can help us avoid regressions as we develop connectors. See the [tests](https://github.com/estuary/connectors/tree/main/tests) directly for more information and examples.
  - [ ] Try to be comprehensive in the tests to include edge cases, such as different data types, different types of tables, hitting certain limits of your connector (e.g. maximum character limits, etc.)
- [ ] You can use the [base-image](https://github.com/estuary/connectors/tree/main/base-image) provided for your connector Docker images.

## Capture connectors
 
- [ ] Check out the protocol definitions of Flow, they include a lot of comments explaining the various interfaces and messages used to communicate between your connector and Flow runtime: [capture.proto](https://github.com/estuary/flow/blob/master/go/protocols/capture/capture.proto)
- [ ] For connectors that work on files or file-like objects, the [filesource](https://github.com/estuary/connectors/tree/main/filesource) provides abstractions to reduce boilerplate work. For an example of a connector implemented using this library see [source-http-file](https://github.com/estuary/connectors/tree/main/source-http-file).
- [ ] For SQL captures, you can use the [sqlcapture](https://github.com/estuary/connectors/tree/main/sqlcapture) library. For an example, see [source-postgres](https://github.com/estuary/connectors/tree/main/source-postgres).
- [ ] When emitting date-time values, if the discovered schema of the connector emits `format: date-time`, the value must be RFC3339 compliant, or otherwise parsing of the value will fail.

## Materialization Connectors

- [ ] Check out the protocol definitions of Flow, they include a lot of comments explaining the various interfaces and messages used to communicate between your connector and Flow runtime: [materialize.proto](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto)
- [ ] You need to choose the right pattern for materialization based on transactional guarantees of your destination technology. This choice is important to uphold exactly-once semantics expected from most connectors. See the comments in [materialize.proto](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto) for more technical details.
  - [ ] If your technology supports committing all data during store phase as part of a transaction, then you can have your destination be authoritative about the checkpoint by also updating the checkpoint as part of the store phase transaction. For an example of this see [materialize-postgres](https://github.com/estuary/connectors/tree/main/materialize-postgres).
  - [ ] If your technology does not support transactions, but does support a retriable idempotent store operation, then you can have the Flow Recovery Log be authoritative and use the idempotency of the operation to ensure exactly-once semantics even in cases of failure. The general idea is to keep track of the operations that are being run in the store phase in the checkpoint sent to the runtime, so that in case the connector fails before it can successfully commit the checkpoint to the recovery log, on the next start of the connector, the operation can be retried by looking at the checkpoint and running the idempotent operations again as part of the Open phase. For an example of this see [materialize-databricks](https://github.com/estuary/connectors/tree/main/materialize-databricks).
  - [ ] There are certain technologies that will not support either of these, in those cases we cannot guarantee exactly-once semantics, but at-least-once. For an example of this see [materialize-google-pubsub](https://github.com/estuary/connectors/tree/main/materialize-google-pubsub).
- [ ] If your technology supports different modes of authentication, the recommended approach is to have a `oneOf` jsonschema under the key `credentials`. See [materialize-databricks](https://github.com/estuary/connectors/blob/main/materialize-databricks/.snapshots/TestSpecification) as an example.
- [ ] We recommend using our [schema-gen](https://github.com/estuary/connectors/tree/main/go/schema-gen) if you are using the golang [invopop/jsonschema](https://github.com/invopop/jsonschema) module to generate JSONSchema for your module as it has some extra quality-of-life improvements.
- [ ] If you want to support network tunnelling access to your technology (at the moment that means connecting through a SSH bastion), you can use the [network-tunnel](https://github.com/estuary/connectors/tree/main/go/network-tunnel) library. See [materialize-postgres](https://github.com/estuary/connectors/tree/main/materialize-postgres) for an example of its usage.
- [ ] If you want to support Google OAuth and Service Account authentication methods, you can use the [auth/google](https://github.com/estuary/connectors/tree/main/go/auth/google) library.

### SQL Materializations

- [ ] The [materialize-sql](https://github.com/estuary/connectors/tree/main/materialize-sql) library abstracts away a lot of shared logic among our SQL materializations. Connector developers will need to implement the various interfaces of this library, and there are [standard implementations](https://github.com/estuary/connectors/blob/main/materialize-sql/std_sql.go) available as well, but they may or may not work with your destination technology. There are many examples of SQL materializations using this library that you can check to get an idea of how they work.

### Materialization Fixtures

You can use `flowctl preview` to run a connector with a fixture, which allows
you to process custom collection data through the materialization connector.
Here is an example of the process using the Motherduck materialization.

First, download the flow binaries, these are required to build most connector
images.  This script will place the binaries into `./flow-bin`:
```
./fetch-flow.sh
```

Build the connector image for your materialization you wish to test:
```
docker build -t ghcr.io/estuary/materialize-motherduck:local --platform=linux/amd64 -f materialize-motherduck/Dockerfile .
```

Write a spec for the test.  This will be similar to the specification used by
flow, so consider using [`flowctl pull-specs`][pull-specs] to get an existing
spec as a starting point:
```yaml
materializations:
  acmeCo/materialize-motherduck:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-motherduck:local"
        config:
          token: <motherduck_service_token>
          database: my_db
          schema: main
          stagingBucket:
            stagingBucketType: S3
            bucketS3: my_bucket
            awsAccessKeyId: <access_key_id>
            awsSecretAccessKey: <secret_access_key>
            region: us-east-1
          syncSchedule:
            syncFrequency: 0s
          advanced:
              no_flow_document: true
              feature_flags: allow_existing_tables_for_new_bindings
    bindings:
      - resource:
          table: tests_simple
        source: tests/simple

collections:
  tests/simple:
    schema:
      type: object
      properties:
        id: { type: integer }
        canary: { type: string }
      required: [id, canary]
    key: [/id]
```

Write the fixture data matching the collection in your above spec.  The file is
a newline-delimited JSON file containing the data for each transaction:

```json
{"commit":true}
["tests/simple", { "id": 1, "canary": "amputation's"}]
["tests/simple", { "id": 2, "canary": "armament's"}]
["tests/simple", { "id": 3, "canary": "splatters"}]
["tests/simple", { "id": 4, "canary": "strengthen"}]
["tests/simple", { "id": 5, "canary": "Kringle's"}]
["tests/simple", { "id": 6, "canary": "grosbeak's"}]
{"commit":true}
["tests/simple", { "id": 7, "canary": "pieced"}]
["tests/simple", { "id": 8, "canary": "roaches"}]
["tests/simple", { "id": 9, "canary": "devilish"}]
["tests/simple", { "id": 10, "canary": "glucose's"}]
{"commit":true}
```

Install [flowctl][], or use the `flowctl` binary downloaded previously in `flow-bin`.

Use `flowctl preview` to run the fixture:
```
RUST_LOG=DEBUG flowctl preview --source ./my-spec.yaml --fixture ./my-fixture.json --output-state --output-apply
```

[flowctl]: https://docs.estuary.dev/guides/get-started-with-flowctl/
[pull-specs]: https://docs.estuary.dev/guides/flowctl/edit-specification-locally/#pull-specifications-locally
