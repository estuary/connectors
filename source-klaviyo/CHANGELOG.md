# Changelog

## 2026-08-05

### Fixed

- Datetime values Klaviyo returns space-separated are now normalized to RFC3339 in every stream. Previously only `profiles` and `global_exclusions` were covered, so `campaigns`, `lists`, `metrics`, `flows`, `email_templates`, archived records, and the free-form `event_properties` of `events` kept the space-separated form, and materializations of fields inferred as `format: date-time` failed with `cannot parse ... as "T"`.
- Free-text values that end in a newline are no longer mistaken for datetimes and rewritten.
