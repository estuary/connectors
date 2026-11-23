# materialize-slack

## 2026-11-23

### Added
- New `Raw Messages` advanced option sends a document's `text` and `blocks` to Slack unescaped, for tasks that put mentions or labeled links in them.

### Fixed
- A `blocks` value that is not an array now fails the task with a descriptive error instead of crashing the connector.

### Changed
- A `blocks` field that cannot hold an array is no longer selected for materialization.
- `&`, `<` and `>` in a document's `text` now appear in Slack as written, so sequences like `<!channel>` and `<url|label>` are no longer turned into mentions or links.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.

## v1, 2022-07-27
- Beginning of changelog.
