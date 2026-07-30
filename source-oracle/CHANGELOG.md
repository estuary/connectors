# Changelog

## 2026-07-30

### Added
- New `rediscovery_interval` advanced option controls how often the connector
  re-runs discovery while a capture is running, to notice schema changes and
  newly added tables. It defaults to 15 minutes.

### Changed
- Captures no longer run discovery twice when they start up. Every restart
  previously issued two rounds of catalog queries in quick succession, which was
  most noticeable when many captures sharing a database all restarted at once.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.
