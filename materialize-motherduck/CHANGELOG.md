# materialize-motherduck

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
