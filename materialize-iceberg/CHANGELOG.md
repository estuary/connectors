# Changelog

## 2026-07-29

### Added
- New advanced option `table_identifier_case` controls the casing of namespace
  and table names created by the materialization. It accepts `lowercase` (the
  default, which folds names to lower case), `uppercase` (folds names to upper
  case, needed for case-sensitive catalogs such as Snowflake's where unquoted
  identifiers resolve upper-case), and `preserve` (keeps names as written in the
  spec). This replaces the never-released `all_caps_table_names` and
  `all_caps_identifiers` options.
- New advanced option `field_name_case` controls the casing of column names
  created by the materialization, using the same values as
  `table_identifier_case`: `preserve` (the default, which names columns exactly
  as the collection names its fields), `lowercase`, and `uppercase`. Columns that
  already exist are matched case-insensitively and are never renamed, so this
  option only affects columns as they are created.

### Deprecated
- The advanced option `lowercase_column_names` is deprecated in favor of
  `field_name_case: lowercase`, which it remains equivalent to. Setting it
  alongside a `field_name_case` of `uppercase` or `preserve` is an error. It is
  no longer offered in the connector's configuration schema, but materializations
  that already set it continue to work.

### Fixed
- The namespace pre-created at apply time now always matches the namespace that
  tables are created in. Previously, a configured namespace containing upper
  case letters or special characters would additionally create an unused
  namespace with the verbatim configured name.
