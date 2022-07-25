# Materialize Rockset

This is a Flow connector that materializes delta updates of each document into a Rockset collection.

## Getting Started

Connector images are available at `ghcr.io/estuary/materialize-rockset`.

The connector configuration must specify a [Rockset API key](https://rockset.com/docs/iam/#api-keys), which can be created in the [Rockset console](https://console.rockset.com/apikeys).

For each Flow collection you'd like to materialize, add a binding with the names of the target Rockset workspace and collection. Both the workspace and collection will be created automatically by the connector if they don't already exist.

**Example flow.yaml:**

```yaml
materializations:
  example/toRockset:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-rockset:dev
        config:
          api_key: <your rockset API key here>
          max_concurrent_requests: 5
    bindings:
      - resource:
          workspace: <your rockset workspace name>
          collection: <your rockset collection name>
        source: example/flow/collection
```

## Bulk ingestion for large backfills of historical data

If you have a large amount of historical data, then Rockset is capable of doing a "bulk ingestion" from S3, and this
connector supports that in several ways. In the `resource` of each binding, you may optionally specify
`initializeFromS3` with the name of an S3 `integration` in Rockset. If you do, then the connector will create the
collection with the given integration, and it will wait for the integration to process all objects in the S3 bucket
before it writes any additional data using Rockset's write API. This is to ensure that documents are always written to
Rockset in the proper order.

The [materialize-s3-parquet](../materialize-s3-parquet/) connector can be used to materialize historical data into S3 in order to facilitate backfilling large collections in Rockset. If this is done, then the materialize-rockset connector can properly pick up where the materialize-s3-parquet connector leaves off. The procedure for doing this is as follows, for the example Flow collection `example/flow/collection`:

1. Follow the [instructions here](https://rockset.com/docs/amazon-s3/#create-an-s3-integration) to create the integration, but _do not create the Rockset collection yet_.
2. Create and activate a materialization of `example/flow/collection` into a unique prefix within an S3 bucket of your choosing.
  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        connector:
          image: ghcr.io/estuary/materialize-s3-parquet:dev
          config:
            bucket: example-s3-bucket
            region: us-east-1
            awsAccessKeyId: <your key>
            awsSecretAccessKey: <your secret>
            uploadIntervalInSeconds: 300
      bindings:
        - resource:
            pathPrefix: example/s3-prefix/
          source: example/flow/collection
  ```
3. You'll need to decide when the S3 materialization is caught up enough to switch over to using the Rockset write API. Once you're ready to make the switch, disable the S3 materialization by setting the shards to disabled in the yaml and re-deploying. This is necessary in order to ensure correct ordering of documents written to Rockset. (see note below on potential improvements)
  ```yaml
  materializations:
    example/toRockset:
      shards:
        disable: true
      # ...the remainder of the materialization yaml remains the same as above
  ```
4. Switch the materialization to use the `materialize-rockset` connector, and re-enable the shards. Here you'll provide the name of the Rockset S3 integration you created above, as well as the bucket and prefix that you previously materialized into. **It's critical that the name of the materialization remains the same as it was for materializing into S3.** Also note that just deleting `disable: true` is enough to cause shards to re-enable once this is deployed.
  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        connector:
          image: ghcr.io/estuary/materialize-rockset:dev
          config:
            api_key: <your rockset API key here>
            max_concurrent_requests: 5
      bindings:
        - resource:
            workspace: <your rockset workspace name>
            collection: <your rockset collection name>
            initializeFromS3:
              integration: <rockset integration name>
              bucket: example-s3-bucket
              region: us-east-1
              prefix: example/s3-prefix/
          source: example/flow/collection
  ```
5. When you activate the new materialization, the connector will create the Rockset collection using the given integration, and wait for it to ingest all of the data from S3 before it continues. During this time, the Flow shards will remain in `STANDBY` status, so `flowctl-admin deploy` is expected to block until the bulk ingestion completes. Once this completes, the materialize-rockset connector will automatically switch over to using the write API.

## Potential improvements

There are a number of additional parameters that users may want to control when creating Rockset collections. The following parameters from the [Rockset API docs](https://rockset.com/docs/rest-api/#createcollection) seem like potential candidates for inclusion in the connector/resource configs.

```
retention_secs
time_partition_resolution_secs
event_time_info
field_mappings
field_mapping_query
clustering_key
field_schemas
inverted_index_group_encoding_options
```
