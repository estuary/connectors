# Changelog

## 2026-11-23

### Changed
- Requests without the configured auth token are rejected before the connector reads their body, so they no longer take up its memory.
- Requests get a 503 response when the connector already holds 256 MiB of request bodies in memory. Senders that retry on errors will resend them once earlier requests finish.
- Requests get a 408 response if sending the body takes longer than 2 minutes.
