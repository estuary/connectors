# source-dynamodb

## 2026-09-01

### Added
- New advanced option `idleBackoffMax` slows polling of open stream shards
  that keep returning no records. After a minute of empty reads the wait
  between polls grows to at most this value, and drops back to once per
  second as soon as records appear. Accepts Go duration strings between
  `1s` and `10m`. At the default of `1s` polling is unchanged.

## 2026-07-30

### Changed
- Captures no longer run discovery twice when they start up.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.
