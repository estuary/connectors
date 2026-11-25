# Changelog

## 2026-11-24

### Changed
- A publish now fails when a binding's query template override uses cursor values but the binding has no cursor columns.

## 2026-11-23

### Added
- New `knownHosts` network tunnel option makes the connector verify the SSH server's host key.

### Changed
- Polling schedules no longer accept negative intervals.

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.
