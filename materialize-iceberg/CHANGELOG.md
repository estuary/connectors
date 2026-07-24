# Changelog

## 2026-07-23

### Added
- New advanced option `identifier_case` controls the casing of namespace and
  table names created by the materialization. It accepts `lowercase` (the
  default, which folds names to lower case), `uppercase` (folds names to upper
  case, needed for case-sensitive catalogs such as Snowflake's where unquoted
  identifiers resolve upper-case), and `preserve` (keeps names as written in the
  spec). This replaces the never-released `all_caps_table_names` and
  `all_caps_identifiers` options.

### Fixed
- The namespace pre-created at apply time now always matches the namespace that
  tables are created in. Previously, a configured namespace containing upper
  case letters or special characters would additionally create an unused
  namespace with the verbatim configured name.
