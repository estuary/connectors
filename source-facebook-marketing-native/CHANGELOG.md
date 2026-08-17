# Changelog

## 2026-08-19

### Fixed
- Ads Insights jobs that Facebook refuses to run are now retried with
  progressively smaller sets of fields. Previously a single ad the API could not
  report on would fail the entire capture, and since the day's cursor only
  advances once every ad succeeds, each restart retried the same day
  indefinitely.

### Changed
- When Facebook will not return a particular field for an ad on a given day, that
  field is now omitted from the record rather than the record being lost. The
  omission is reported in the connector logs.
