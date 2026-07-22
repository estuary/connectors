# source-sqlserver-ct

## 2026-07-22

### Changed

- Change polling now uses a single batched query to identify tables with new changes prior
  to reading out those changes. This improves efficiency when capturing mostly-idle tables.
