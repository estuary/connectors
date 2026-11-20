# Changelog

## 2026-11-11

### Added
- Initial release of the Azure Cosmos DB for NoSQL capture connector, reading the change feed in latest version mode.
- Authentication under `credentials` with an account key or Microsoft Entra service principal client secrets using Cosmos DB data-plane RBAC.
- Items without a partition key value, or with a `null` value, are captured with a non-null default at the partition key path (`""`, `0`, or `false`). A `/_meta/partitionKeyState` key field records whether each path had a value, an explicit `null`, or no value, so the collections can be materialized.
- Integer partition keys use the same collection key for equivalent values such as `42` and `42.0`.
- Discovery and capture of a single `database`, an explicit `databases` list, or `all_databases` in an account, with independent bindings for same-named containers.
- Optional request priority, throughput bucket, and change feed page size controls.
- Configurable `Beginning`, `Now`, or timestamp start positions. Saved continuations take precedence on restart.
