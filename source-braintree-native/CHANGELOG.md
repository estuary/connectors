# Changelog

## 2026-08-04

### Fixed
- Reduced memory use while backfilling. Pages of fetched records were held in memory for the duration of a date window instead of being released as they were emitted, which could exhaust a capture's memory on high volume streams.

## 2026-07-29

### Fixed
- Transient Braintree search timeouts (`HTTP 422` responses with a `timeout` reason) are now retried instead of failing the capture.

## 2026-07-27

### Added
- A per-binding `Concurrency` setting that controls the maximum number of concurrent requests used to fetch a stream. Lower it if Braintree reports elevated errors or throttles requests. New bindings default to 5 for `disputes` and 20 for other search-based streams.
