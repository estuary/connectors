# materialize-webhook

## 2026-10-23

### Added
- Webhooks can send each document's selected fields, such as `flow_published_at`, instead of the full document.
- FEATURE FLAG SWITCH: Starting 2026-10-30 the feature flag field_selection will be on by default for new tasks, while existing tasks created at or before 2026-10-29 will keep their existing behaviour. Each webhook payload contains the binding's selected fields instead of the full document.

## v1, 2022-07-27
- Beginning of changelog.
