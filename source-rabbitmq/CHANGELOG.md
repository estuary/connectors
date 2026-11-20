# Changelog

## 2026-11-15

### Added
- Initial release of the RabbitMQ capture connector with support for classic, quorum, and stream queues over AMQP 0-9-1.
- Classic and quorum messages are acknowledged only after Estuary commits them, so capture is at least once and `_meta.redelivered` marks messages RabbitMQ delivered again. Stream queues resume from a saved offset and stop with instructions to backfill into a new collection if RabbitMQ delivers an offset below it.
- Discovery of classic, quorum, and stream queues through the management API, skipping other queue types and server-named temporary queues.
- Custom TLS CA bundles and client certificates for AMQP and HTTPS discovery, with optional SASL EXTERNAL certificate authentication for AMQP.
- Configurable per-queue prefetch with a default of 10.
- Initial stream positions of `last`, `next`, RFC3339 timestamps, and RabbitMQ intervals, while retaining `first` as the default and always resuming saved offsets.
- Capture of AMQP content type, content encoding, correlation ID, reply destination, message type, application ID, user ID, priority, delivery mode, and expiration in `_meta`. Numeric properties preserve zero values.
- Non-UTF-8 AMQP strings and header names are preserved with collision-safe encoding, including in document IDs.
