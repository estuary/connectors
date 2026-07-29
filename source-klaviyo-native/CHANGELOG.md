# Changelog

## 2026-07-30

### Fixed
- Datetime values Klaviyo returns space-separated (consent timestamps, the events datetime cursor, and custom properties) are now normalized to RFC3339, so materializations of fields inferred as `format: date-time` no longer fail with `cannot parse ... as "T"`.
