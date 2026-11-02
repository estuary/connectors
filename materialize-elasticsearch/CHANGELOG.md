# materialize-elasticsearch

## 2026-10-22

### Fixed
- Retried requests, such as Loads retried after Elasticsearch pushes back with a 429, sent an empty body and failed the task.

## v3, 2023-08-21
- Create index mappings based on field selection.
- Move "number of replicas" configuration for new indices to an advanced, optional, endpoint-level configuration.
- Make "number of shards" resource configuration optional.

## v2, 2023-04-24
- Use dynamic runtime mappings.
- Allow authentication using API keys.

## v1, 2022-07-27
- Beginning of changelog.
