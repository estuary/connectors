# Changelog

## 2026-07-18

### Added
- Initial release of the Amazon SQS capture connector. Captures messages
  from standard and FIFO queues, deleting each message from the queue
  after it has been durably committed. FIFO messages are captured in
  per-group queue order.
