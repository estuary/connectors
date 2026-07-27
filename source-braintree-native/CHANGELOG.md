# Changelog

## 2026-07-27

### Added
- A per-binding `Concurrency` setting that controls the maximum number of concurrent requests used to fetch a stream. Lower it if Braintree reports elevated errors or throttles requests. New bindings default to 5 for `disputes` and 20 for other search-based streams.
