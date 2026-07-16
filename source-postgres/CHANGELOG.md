# Changelog

## 2026-07-16

### Fixed
- Replication slot creation is exempted from `statement_timeout`, so that slot
  creation blocks indefinitely rather than timing out when there are long-running
  transactions open (until they commit and slot creation finishes).

## 2026-07-14

### Changed
- Queries now use a default `statement_timeout` of 2 minutes when the
  `statement_timeout` advanced option is left blank. Set to `0` for the
  previous behavior of disabling timeouts.
