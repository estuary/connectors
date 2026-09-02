# source-snowflake

## 2026-09-02

### Fixed
- Dynamic table discovery is now scoped to the capture database. Previously a
  same-named dynamic table in another database could cause a captured table to
  disappear from discovery.

## 2026-07-30

### Changed
- Captures no longer run discovery twice when they start up.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.
