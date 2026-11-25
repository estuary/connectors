# Changelog

## 2026-11-24

### Changed
- A publish now fails when a binding's query template override uses cursor values but the binding has no cursor columns.

## 2026-11-23

### Added
- New `knownHosts` network tunnel option makes the connector verify the SSH server's host key.
- New `skipHostKeyVerification` network tunnel option connects without verifying the SSH server's host key.

### Changed
- Polling schedules no longer accept negative intervals.
- Network tunnels now require `knownHosts` unless `skipHostKeyVerification` is set.

## 2026-11-19

### Fixed
- Table discovery now passes `discover_schemas` entries as bound parameters, so a schema name containing `'` can be selected.

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.
