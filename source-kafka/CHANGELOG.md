# Changelog

## 2026-11-02

### Changed
- Sharded captures now spread each topic's partitions evenly across shards.

### Fixed
- Captures now pick up partitions added to a topic within minutes instead of after their next restart.
