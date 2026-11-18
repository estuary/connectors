---
description: Capture InfluxDB time series with incremental timestamp polling, optional lookback, configurable authentication, and TLS.
---

# InfluxDB

This connector captures time series points from [InfluxDB](https://www.influxdata.com/) 2.x into Estuary collections using the HTTP API and Flux queries.

## Supported data resources

Each bucket and measurement pair is discovered as a separate resource, normally named `<bucket>.<measurement>`, such as `sensors.readings`. Each resource is mapped to an Estuary collection through a separate binding. The connector lists readable buckets using Flux `buckets()`, including when the token has access to only some of the organization's buckets. System buckets whose names begin with `_` are excluded.

Bucket and measurement names can both contain dots, such as bucket `sensors.west` and measurement `app.requests`. Discovery stores their exact values in separate `bucket` and `measurement` fields. For dotted measurements only, the discovered `name` uses URL-escaped components separated by `/`, with dots escaped as `%2E`: `sensors%2Ewest/app%2Erequests`. This keeps resource names unique, and resource paths are `[name]`. Use the discovered name unchanged. Bindings containing only `name` and `interval` are also accepted: the final dot in `name` separates the bucket and measurement. Publishing validates the connection and each binding's bucket access. Measurements with no retained data remain valid, and existing bindings continue polling them.

### Documents

The connector combines fields with the same measurement, tags, and timestamp into one document:

```json
{
  "time": "2026-01-01T12:00:00.123456789Z",
  "measurement": "readings",
  "tags": {"host": "a", "region": "west"},
  "fields": {"value": 1.5, "count": 7, "ok": true, "state": "up"},
  "series": "readings,host=a,region=west"
}
```

`time` is an RFC3339 timestamp in UTC, with nine fractional digits when the fractional second is nonzero. Tag values are strings. Field values retain their InfluxDB types: floating point numbers, integers, booleans, and strings.

`series` contains the measurement followed by tags sorted by tag name. Backslashes, commas, and spaces are escaped with a backslash; equals signs in tag names and values are also escaped. The collection key is `["/series", "/time"]`.

### Incremental capture

The first capture reads points on or after an explicit `start_date`, preserving nanosecond precision. When omitted, `start_date` initializes the cursor 30 days before the first run; lookback may read earlier. Reads are ordered by timestamp and divided into windows of at most one hour. At most four windows are read concurrently across all bindings. Windows exceeding 50,000 field rows are retried with shorter ranges before emitting documents; sparse windows can grow back to one hour. All fields and series at a timestamp stay together, so a single timestamp can exceed the row target. Once caught up, each binding waits for its configured interval before polling for newer points, every five minutes by default. Points at or after the current time minus `advanced.lag_seconds` are deferred to a later poll; the default lag is 60 seconds.

The connector checkpoints the newest point timestamp it has captured for each binding. By default, resuming a capture reads strictly after that timestamp, preserving nanosecond precision and excluding points already captured at the cursor. An empty poll leaves the cursor unchanged.

Set `advanced.lookback_hours` to a positive integer to re-read points starting that many hours before the cursor on each poll, including the cursor timestamp itself. An explicit `start_date` limits lookback. When `start_date` is omitted, lookback follows the saved cursor across restarts, even if that cursor is older than 30 days, and can read before the initial default start date. Reads never go before InfluxDB's minimum timestamp. For example, a cursor at 14:00 and a two-hour lookback re-read from 12:00, followed by new points up to the lag cutoff. This captures buffered arrivals, overwritten fields, and fields added to existing points within the lookback range. Repeated documents have the same `series` and `time` key, so updates merge into the same collection records. Replay checkpoints never move the cursor backward, and each poll finishes its bounded windows before waiting for the binding interval. A restart may replay the range again. Larger lookbacks increase query load and the number of documents emitted.

:::warning
InfluxDB has no write-time index. With the default zero-hour lookback, points written later with timestamps older than or equal to the capture cursor are not captured. With lookback enabled, points and field changes older than the lookback range are still not captured. Deletions are not captured. Increase `advanced.lag_seconds` to allow for expected ingestion delays before timestamps are read, or enable `advanced.lookback_hours` for delayed arrivals and corrections. To capture older historical corrections, re-backfill the affected binding with a suitable `start_date`.
:::

## Prerequisites

- An endpoint that supports the v2 HTTP API and Flux, including the `buckets()` function. Authentication options do not change the query protocol: native InfluxDB 1.x InfluxQL and InfluxDB 3 SQL/InfluxQL APIs are not implemented.
- The instance URL and organization name.
- Credentials with permission to list and read the buckets you want to capture. Write permission is not required.
- Network access from Estuary to the InfluxDB instance. Use HTTPS for connections over public networks.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the InfluxDB source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/url`** | URL | InfluxDB instance URL, including scheme and port if needed, such as `https://us-east-1-1.aws.cloud2.influxdata.com` or `http://host:8086`. | string | Required |
| `/token` | API Token | Secret API token with read access to the selected buckets, sent as `Authorization: Token <token>`. | string | Required unless `credentials` is supplied |
| `/credentials` | Authentication | Alternative Basic or Bearer authentication. Supply exactly one of `token` or `credentials`. | object | Optional |
| `/credentials/credentials_title` | Authentication Method | `Username & Password` for Basic authentication, or `Database Token` for Bearer authentication. | string | Required with `credentials` |
| `/credentials/username` | Username | InfluxDB username, without a colon. | string | Required for `Username & Password` |
| `/credentials/password` | Password | Secret InfluxDB password. | string | Required for `Username & Password` |
| `/credentials/access_token` | Database Token | Secret database token sent as `Authorization: Bearer <token>`. | string | Required for `Database Token` |
| **`/org`** | Organization | InfluxDB organization name. | string | Required |
| `/start_date` | Start Date | Capture points on or after this RFC3339 date and time, preserving up to nine fractional digits. When omitted, the default initializes the cursor but does not limit lookback. Dates before InfluxDB's minimum time, `1677-09-21T00:12:43.145224194Z`, start at that minimum. Changing it does not reset an existing cursor; re-backfill the binding to reread history. | string, date-time | 30 days before the first run |
| `/advanced/lag_seconds` | Lag Seconds | Nonnegative delay in seconds before points become eligible for capture. | integer | `60` |
| `/advanced/lookback_hours` | Lookback Hours | Nonnegative hours to re-read before the newest captured point on every poll, bounded by an explicit `start_date`. When `start_date` is omitted, lookback follows the saved cursor across restarts. Zero preserves the default behavior of reading strictly after the cursor. | integer | `0` |
| `/advanced/ssl_ca_certificate` | SSL CA Certificate | PEM-encoded CA certificate or bundle to trust in addition to public CAs. Server hostname verification remains enabled. | string | Empty; public CAs only |
| `/advanced/ssl_skip_verify` | Skip SSL Verification | Disable certificate and hostname verification for HTTPS. Prefer supplying a CA certificate; use this option only on trusted networks. Cannot be combined with `ssl_ca_certificate`. | boolean | `false` |

Username & Password sends Basic authentication in the HTTP Authorization header and works only with a native InfluxDB 1.x server with Flux enabled. Compatibility with specific InfluxDB 1.x versions has not been verified. InfluxDB 2.x OSS and Cloud v1-compatibility usernames and passwords are not accepted on `/api/v2/query`; use an API token there. Bearer authentication provides the header scheme used by InfluxDB 3 database tokens; it does not add native InfluxDB 3 querying. Compatibility with specific managed services has not been verified.

For Basic authentication, replace `token` with:

```yaml
credentials:
  credentials_title: Username & Password
  username: telegraf-reader
  password: <secret>
```

For Bearer authentication, replace `token` with:

```yaml
credentials:
  credentials_title: Database Token
  access_token: <secret>
```

For an internal CA or self-signed certificate, paste the PEM into the configuration:

```yaml
advanced:
  ssl_ca_certificate: |
    -----BEGIN CERTIFICATE-----
    <base64-encoded certificate>
    -----END CERTIFICATE-----
```

TLS options apply to discovery, validation, and capture requests. Certificate verification remains enabled unless explicitly disabled.

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Discovered bucket and measurement name, such as `sensors.readings`. | string | Required |
| `/bucket` | Bucket | InfluxDB bucket name, including any dots. Discovered resources always include this field. | string | Required with `measurement`; otherwise inferred from `name` |
| `/measurement` | Measurement | InfluxDB measurement name, including any dots. Discovered resources always include this field. | string | Required with `bucket`; otherwise inferred from `name` |
| `/interval` | Interval | Interval between syncs, as an ISO 8601 duration. | string | `PT5M` |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-influxdb:v1
        config:
          url: https://us-east-1-1.aws.cloud2.influxdata.com
          token: <secret>
          org: my-organization
          start_date: "2026-01-01T00:00:00Z"
          advanced:
            lag_seconds: 60
            lookback_hours: 2
    bindings:
      - resource:
          name: sensors.readings
          bucket: sensors
          measurement: readings
          interval: PT5M
        target: ${PREFIX}/readings
      - resource:
          name: sensors.status
          bucket: sensors
          measurement: status
          interval: PT5M
        target: ${PREFIX}/status
```
