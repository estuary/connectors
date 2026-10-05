# materialize-motherduck

## 2026-10-02

### Fixed
- A DuckLake destination could fail a publication or restart a task with
  `Failed to commit DuckLake transaction` when another writer committed to the
  same database at the same moment. Creating, altering, dropping, and
  truncating tables, creating schemas, and starting a task now retry this
  conflict, waiting a little longer before each attempt.
- Retries of a failed data commit now wait before trying again, making them
  less likely to collide with the same concurrent writer.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-10

### Fixed
- Changing the type of a materialized column in a DuckLake destination failed
  with `Cannot change type of column ... only widening type promotions are
  allowed`, and because the change is applied while the task recovers, the
  materialization then failed on every restart. DuckLake never rewrites existing
  data files, so its in-place `ALTER COLUMN ... TYPE` accepts only lossless
  widening promotions. Column type changes in a DuckLake destination are now
  made by adding a column of the new type, converting the existing values into
  it, and renaming it over the original. Classic MotherDuck databases continue
  to use the in-place change.

## v1, 2023-10-31
- Beginning of changelog.
