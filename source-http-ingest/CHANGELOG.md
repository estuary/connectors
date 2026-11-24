# Changelog

## 2026-11-23

### Changed
- Requests without the configured auth token are rejected before the connector reads their body, so they no longer take up its memory.
