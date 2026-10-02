# source-kafka

## 2026-10-02

### Added
- `advanced.feature_flags`: a comma-separated list of flags for Estuary support to enable or disable experimental behavior. The first flag, `protobuf_transcoder`, switches protobuf payloads to a faster decoder that produces identical documents. It is off by default.

### Fixed
- A protobuf message with a malformed Confluent message-index header now fails with a clear error instead of crashing the connector.
