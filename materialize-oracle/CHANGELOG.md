# Changelog

## 2026-11-11

### Added
- Materialize Estuary collections into Oracle Database tables with standard updates, delta updates, and optional hard deletes.
- Preserve document UUIDs in standard-update loads through the root document or a projection of `/_meta`.
- Support switching between standard and delta updates through a binding backfill that recreates the table.
- Validate column names against Oracle's uppercase `ROWID` restriction and reserve space for temporary columns used during string migrations.
