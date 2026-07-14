# Changelog

## 2026-07-14

### Changed
- Queries now use a default `statement_timeout` of 2 minutes when the
  `statement_timeout` advanced option is left blank. Set to `0` for the
  previous behavior of disabling timeouts.
