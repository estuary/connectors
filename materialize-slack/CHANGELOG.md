# materialize-slack

## 2026-11-23

### Fixed
- A `blocks` value that is not an array now fails the task with a descriptive error instead of crashing the connector.

### Changed
- A `blocks` field that cannot hold an array is no longer selected for materialization.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.

## v1, 2022-07-27
- Beginning of changelog.
