# materialize-google-sheets

## 2026-11-05

### Fixed
- A Google Sheets server error while writing rows can no longer insert the rows twice.
- Rate-limited requests now give up after 10 minutes instead of retrying forever.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.

## v2, 2022-09-16

- Add OAuth 2.0 authentication option.

## v1, 2022-07-27

- Beginning of changelog.
