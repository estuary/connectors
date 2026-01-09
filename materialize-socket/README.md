# Source Socket Connector

An Estuary Flow source connector for capturing data from generic TCP/TLS socket connections.

## Overview

This connector captures streaming data from TCP socket endpoints with support for:

- **Plain TCP** and **TLS/SSL** encrypted connections
- **Multiple codecs**: Plain text, line-delimited, and JSON
- **Automatic reconnection** with configurable retry logic
- **Checkpointing** for exactly-once semantics

## Configuration

### Endpoint Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `host` | string | Yes | - | Hostname or IP address to connect to |
| `port` | integer | Yes | - | Port number (1-65535) |
| `tls.enabled` | boolean | No | false | Enable TLS/SSL encryption |
| `tls.verify_cert` | boolean | No | true | Verify server certificate |
| `tls.ca_cert` | string | No | - | CA certificate file path or PEM string |
| `tls.client_cert` | string | No | - | Client certificate for mutual TLS |
| `tls.client_key` | string | No | - | Client private key for mutual TLS |
| `tls.server_hostname` | string | No | - | Expected server hostname for verification |
| `codec` | string | No | "line" | Data codec: `plain`, `line`, or `json` |
| `buffer_size` | integer | No | 65536 | Read buffer size in bytes |
| `reconnect_delay` | float | No | 5.0 | Seconds to wait before reconnecting |
| `max_reconnect_attempts` | integer | No | -1 | Max reconnect attempts (-1 = unlimited) |
| `connection_timeout` | float | No | 30.0 | Connection timeout in seconds |
| `idle_timeout` | float | No | null | Close after N seconds of no data |

### Example Configuration

```yaml
captures:
  acmeCo/socket-capture:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-socket:dev
        config:
          host: data.example.com
          port: 9000
          tls:
            enabled: true
            verify_cert: true
          codec: json
          reconnect_delay: 10.0
    bindings:
      - resource:
          stream: socket_events
        target: acmeCo/socket-events
```

## Codecs

### Plain Codec (`plain`)

Emits raw data as strings without any parsing. Best for binary-to-text protocols or when you want to handle parsing downstream.

### Line Codec (`line`)

Splits incoming data on newline characters (`\n` or `\r\n`). Best for log streams, syslog, or any line-oriented protocol.

Output document structure:
```json
{
  "data": {"value": "the line content"},
  "raw": "the line content",
  "timestamp": "2024-01-15T10:30:00Z",
  "sequence": 1,
  "session_id": "abc123"
}
```

### JSON Codec (`json`)

Parses newline-delimited JSON (NDJSON). Each line is expected to be a valid JSON object.

Output document structure:
```json
{
  "data": {"your": "json", "object": "here"},
  "timestamp": "2024-01-15T10:30:00Z",
  "sequence": 1,
  "session_id": "abc123"
}
```

Malformed JSON lines produce error documents:
```json
{
  "data": {
    "_error": true,
    "_error_message": "Expecting value: line 1 column 1 (char 0)",
    "_raw": "invalid json here"
  }
}
```

## TLS Configuration

### Basic TLS (Server Verification)

```yaml
config:
  host: secure.example.com
  port: 443
  tls:
    enabled: true
    verify_cert: true
```

### TLS with Custom CA

```yaml
config:
  host: internal.example.com
  port: 9443
  tls:
    enabled: true
    verify_cert: true
    ca_cert: /path/to/ca.crt
```

### Mutual TLS (mTLS)

```yaml
config:
  host: mtls.example.com
  port: 8443
  tls:
    enabled: true
    verify_cert: true
    client_cert: /path/to/client.crt
    client_key: /path/to/client.key
```

### Insecure TLS (Not Recommended)

```yaml
config:
  host: dev.example.com
  port: 443
  tls:
    enabled: true
    verify_cert: false
```

## Document Schema

Each captured document includes:

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | datetime | When the data was received |
| `source_host` | string | Source hostname |
| `source_port` | integer | Source port |
| `data` | object | Parsed data (structure depends on codec) |
| `raw` | string | Raw string for non-JSON codecs |
| `sequence` | integer | Sequence number within session |
| `session_id` | string | Unique connection session identifier |

## Development

### Running Tests

```bash
pip install -e ".[dev]"
pytest
```

### Building Docker Image

```bash
docker build -t source-socket:dev .
```

## License

Dual licensed under MIT or Apache 2.0 at your discretion.