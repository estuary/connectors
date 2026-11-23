# Changelog

## 2026-11-23

### Added
- New `knownHosts` network tunnel option makes the connector verify the SSH server's host key.

### Changed
- Polling schedules no longer accept negative intervals.
- Network tunnels now require `knownHosts`.

## 2026-11-19

### Fixed
- Table discovery now passes `discover_schemas` entries as bound parameters, so a schema name containing `'` can be selected.

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.
