# Changelog

## 2026-11-11

### Added
- Materialize Estuary collections into Azure Cosmos DB for NoSQL containers with standard updates, delta updates, and hard deletes.
- Encode null key values as `~N`, distinct from the string `"null"`.
- Escape Unicode control characters as uppercase UTF-8 byte sequences in item IDs.
