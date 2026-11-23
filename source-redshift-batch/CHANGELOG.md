# Changelog

## 2026-11-23

### Changed
- Polling schedules no longer accept negative intervals.

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.
