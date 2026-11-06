# Changelog

## 2026-08-05

### Fixed
- The `Connection String` credential field is now marked as secret, so the
  dashboard masks its value and encrypts it when the configuration is saved.
  Previously the connection string, which includes the storage account key, was
  stored as plain text. Existing captures will be encrypted the next time their
  configuration is saved.
