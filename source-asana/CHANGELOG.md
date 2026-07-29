# Changelog

## 2026-07-29

### Fixed
- `portfolios` documents with members or project templates no longer fail collection schema
  validation — `members` and `project_templates` are now correctly declared as arrays.
- `portfolios` `due_on` and `start_on` are now declared as dates rather than date-times.
