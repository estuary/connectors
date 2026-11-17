# Changelog

## 2026-11-14

### Fixed
- `live_specs`, `task_status`, `publication_history`, `storage_mappings`, `alert_subscriptions`, `service_accounts` and the catalog stats streams capture data again after an Estuary API change caused them to come back empty.

## 2026-10-21

### Fixed
- `alert_subscriptions` captures admins' alert subscriptions again instead of coming back empty.
- `alert_subscriptions` captures alert subscriptions on prefixes where the API key has team admin access without full admin access.

## 2026-10-07

### Added
- Initial release of the Estuary capture connector.
