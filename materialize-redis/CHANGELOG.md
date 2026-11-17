# Changelog

## 2026-11-11

### Added
- SSH bastion connections through `networkTunnel`, including forwarding discovered Sentinel primaries.
- Custom CA certificates, mutual TLS, a server-name override, and optional certificate verification bypass using shared TLS helpers.
- Sentinel primary discovery and failover with separate Sentinel authentication.
- Optional RedisJSON output with standard and delta updates, expiration, and hard deletes for search-indexable documents.
- Optional JSON-array key encoding to prevent component and type collisions, with migration validation for existing prefixes.
- Materialize collections into Redis as JSON strings, with standard and delta updates, key expiration, and hard deletes.
- Connect to Redis using username and password authentication and TLS.

