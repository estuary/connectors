# Materialize Kafka

A Flow materialization connector that publishes Flow collection documents to Apache Kafka topics as JSON or Avro messages.

## What it does

This connector materializes Flow collections into Kafka topics, supporting both JSON and Avro message formats. It handles:

- **Topic Management**: Automatically creates topics with configurable partitions and replication factor
- **Message Encoding**: Supports JSON and Avro formats with schema registry integration
- **Authentication**: SASL authentication with multiple mechanisms (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- **TLS/SSL**: Secure connections using system certificates
- **Key Handling**: Materializes collection keys as Kafka message keys for proper partitioning

## Architecture

Built in Rust using the `rdkafka` crate, the connector implements the Flow materialization protocol:

- **Protocol Handler** (`lib.rs`): Main entry point handling spec/validate/apply/open requests
- **Configuration** (`configuration.rs`): Endpoint and resource configuration with JSON schema definitions
- **Validation** (`validate.rs`): Connectivity checks and field constraint validation
- **Application** (`apply.rs`): Topic creation and management
- **Transaction Processing** (`transactor.rs`): Real-time message publishing with Flow protocol handling
- **Schema Management** (`binding_info.rs`): Avro schema generation and registry integration

### Protocol Implementation

Implements Flow materialization protocol v3032023 with delta updates mode enabled. The transaction lifecycle:

1. **Spec**: Returns configuration schemas and connector metadata
2. **Validate**: Checks Kafka/schema registry connectivity, returns field constraints
3. **Apply**: Creates missing Kafka topics (idempotent)
4. **Open**: Begins transaction processing
5. **Transaction Cycle**:
   - **Flush**: Signals end of load phase (no-op for Kafka)
   - **Store**: Documents immediately published as Kafka messages with delivery tracking
   - **StartCommit**: Explicit delivery confirmations awaited (fail-fast on errors)
   - **Acknowledge**: Transaction completion confirmed

### Delivery Guarantees

The connector uses a custom `AckTrackingContext` to ensure strong delivery guarantees:

- **Producer Configuration**: Sets `acks=-1` (all in-sync replicas) and `enable.idempotence=true`  
- **Explicit ACK Tracking**: Tracks pending vs. completed message deliveries with monotonic counters
- **Fail-Fast Semantics**: `StartCommit` fails immediately on first delivery failure
- **Guaranteed Delivery**: `StartCommit` only sent after ALL messages confirmed delivered
- **Error Propagation**: Returns actual `KafkaError` details for proper error handling
- **Timeout Visibility**: Uses default 5-minute message timeout to surface operational issues

This eliminates the need for `producer.flush()` and provides stronger consistency than flush-only approaches, which only wait for messages to be sent, not confirmed delivered.

### Delta Updates Mode

The connector operates in delta updates mode, meaning it only processes document changes within each transaction without loading existing documents. This is optimal for Kafka's append-only semantics where the Load/Loaded protocol phases are skipped.

### Message Flow

Collection documents are converted to Kafka messages:
- Keys from Flow collection keys for proper partitioning
- Values from selected fields plus optional root document
- JSON or Avro encoding based on configuration

### Limitations

- Avro format requires a schema registry
- Schema subjects use a hash-based naming strategy (`{topic}-{schema_hash}`)
- No support for custom partitioning strategies beyond key-based
- Root document fields are always serialized as strings in Avro

## Essential Types

### EndpointConfig
- `bootstrap_servers`: Kafka cluster connection string
- `credentials`: Optional SASL authentication
- `tls`: TLS configuration 
- `message_format`: JSON or Avro
- `schema_registry`: Required for Avro format
- `topic_partitions`/`topic_replication_factor`: Topic creation settings

### Resource  
- `topic`: Target Kafka topic name

### BindingInfo
Runtime binding state including topic name, Avro schemas (if applicable), key pointers, and field names.

## Getting Started

1. **Build**: `cargo build --release`
2. **Test**: `cargo test` 
3. **Docker**: `docker build -t materialize-kafka .`

The connector expects Flow protocol messages on stdin and responds on stdout. Use `flowctl` for local development and testing.

For Avro format, ensure your schema registry is accessible and credentials are properly configured. JSON format works without additional dependencies.