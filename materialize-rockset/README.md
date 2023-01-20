# Materialize Rockset

This is a Flow connector that materializes delta updates of each document into a Rockset collection.

## Getting Started

Connector images are available at `ghcr.io/estuary/materialize-rockset`.

The connector configuration must specify a [Rockset API key](https://rockset.com/docs/iam/#api-keys), which can be created in the [Rockset console](https://console.rockset.com/apikeys). You must also specify a [Region base URL](https://rockset.com/docs/rest-api/#introduction) where you desired deployment is active.

For each Flow collection you'd like to materialize, add a binding with the names of the target Rockset workspace and collection. Both the workspace and collection will be created automatically by the connector if they don't already exist.

**Example flow.yaml:**

```yaml
materializations:
  example/toRockset:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-rockset:dev
        config:
          region_base_url: <ex: api.usw2a1.rockset.com>
          api_key: <your rockset API key here>
    bindings:
      - resource:
          workspace: <your rockset workspace name>
          collection: <your rockset collection name>
        source: example/flow/collection
```

## Potential improvements

Rockset supports a [`field_mapping_query`](https://rockset.com/docs/rest-api/#createcollection) when creating a collection. This can allow for specifying things like `_event_time` mappings and custom field mappings. This is not currently supported by the Flow materialization, but could be a potential enhancement in the future.