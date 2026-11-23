# Changelog

## 2026-11-23

### Fixed
- Keys shorter than 5 bytes that start with a 0 byte are captured as base64 under `_key` instead of crashing the connector. So are empty keys.

## 2026-11-03

### Fixed
- The connector keeps reading while the Kafka client reconnects to a disconnected broker instead of exiting. This may reduce task failures on clusters whose brokers sometimes close connections.

## 2026-11-02

### Changed
- Sharded captures now spread each topic's partitions evenly across shards.

### Fixed
- Captures now pick up partitions added to a topic within minutes instead of after their next restart.
