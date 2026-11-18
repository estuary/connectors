---
description: Materialize Estuary collections as time series points in InfluxDB 2.x for dashboards and alerts, with configurable timestamps, tags, and source deletions.
---

# InfluxDB

This connector materializes Estuary collections as points in an existing InfluxDB 2.x bucket. Each binding writes to a measurement, with selected collection fields mapped to tags and fields.

The connector supports InfluxDB OSS 2.x and InfluxDB Cloud backed by the 2.x engine. InfluxDB 1.x and 3.x, including Cloud Serverless, Cloud Dedicated, and Core, are not supported.

## Prerequisites

To use this connector, you'll need:

* An InfluxDB 2.x instance reachable from Estuary over HTTP or HTTPS.
* An existing organization and bucket. The connector checks that the bucket exists and does not create buckets.
* An API token with permission to read the bucket's metadata and write points. The bucket's write permission also permits hard deletes.
* At least one Estuary collection to materialize.

Custom certificate authorities and mutual TLS are not supported.

## Configuration

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/url`** | InfluxDB URL | Base URL of the InfluxDB 2.x instance, such as `https://us-east-1-1.aws.cloud2.influxdata.com` or `http://host:8086`. | string | Required |
| **`/token`** | API Token | Secret API token used to authenticate with InfluxDB. | string | Required |
| **`/org`** | Organization | Organization name. | string | Required |
| **`/bucket`** | Bucket | Name of the existing bucket that receives points from all bindings. | string | Required |
| `/hardDelete` | Hard Delete | Delete matching points when a document is deleted in the source. When disabled, deletion documents are written as ordinary points. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/measurement`** | Measurement | Measurement that receives this collection's points. | string | Required |
| `/timestampField` | Timestamp Field | Projection field containing each point's time. Must be an always-present string with the `date-time` format. | string | `flow_published_at` |
| `/tagFields` | Tag Fields | Projection fields to write as tags. Objects and arrays cannot be tags. Set to `[]` to write no tags. | array of strings | Collection key fields other than the timestamp field |

Use projection field names, such as `sensor` or `event_time`, for `timestampField` and `tagFields`.

### Sample

```yaml
materializations:
  acmeCo/sensor_metrics:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-influxdb:v1
        config:
          url: https://us-east-1-1.aws.cloud2.influxdata.com
          token: <your-api-token>
          org: acmeCo
          bucket: sensors
          hardDelete: true
    bindings:
      - resource:
          measurement: readings
          timestampField: ts
          tagFields: [sensor]
        source: acmeCo/readings
```

For a collection keyed by `/sensor` and `/ts`, the document below writes a point in the `readings` measurement, tagged with `sensor=s1`, at the specified timestamp. `temp` and `ok` become fields.

```json
{"sensor":"s1","ts":"2026-01-01T00:00:00.123456789Z","temp":20.5,"ok":true}
```

## Timestamps, tags, and fields

Timestamps are parsed as RFC3339 with nanosecond precision. The default, `flow_published_at`, uses the document's publication time in Estuary. An update published at a new time therefore creates another point. Choose an event timestamp when points should use the time the event occurred.

The timestamp and tag fields must be selected by their configured names. Every other selected field becomes an InfluxDB field, using its projection name. The timestamp and tags are not duplicated as fields.

Objects without nested fields in their schema are always written as JSON. The connector requires them, because the default field selection would not select them and their values would be lost, so they cannot be excluded. An object whose schema defines nested fields depends on the binding's `recommended` field selection setting. With `recommended: true`, it is written as those individual fields. With a numeric depth such as `recommended: 1`, a top-level object is written as JSON and its nested fields are not written separately. Require such an object field to write its JSON value, or exclude it to omit it. With a numeric depth, a nested object without nested fields of its own is written both as its own field and inside its parent's JSON.

| Collection value | InfluxDB representation |
|---|---|
| Integer | Signed 64-bit integer field |
| Number | Floating-point field |
| Boolean | Boolean field |
| String | String field |
| Object or array | Compact JSON in a string field |
| Full document (`flow_document`), when selected | JSON in a string field |
| Null or missing field | Omitted |

Tag values are strings. Integer tags use decimal notation, and boolean tags use `true` or `false`. Missing or null tags are omitted. A document with no non-null fields remaining after removing its timestamp and tags is skipped with a warning.

## Updates and deletes

This connector uses only [delta updates](/concepts/materialization/#delta-updates). It does not load existing points before writing. Writes are sent in batches of at most 5,000 points and about 8 MB, and each transaction waits for InfluxDB to accept all of its batches.

A point's identity is its measurement, tag set, and timestamp. A point with the same series and time is merged, so a field missing from a newer document keeps its older value. Send all fields again when replacing a point's values. Omitting a field or setting it to null does not remove its earlier value.

With `hardDelete: true`, source deletion documents, including documents with `_meta.op` set to `d`, delete points matching their measurement and key tag values:

* If the timestamp field is part of the collection key, the connector deletes only that instant. Points at adjacent nanoseconds are preserved.
* Otherwise, it deletes matching points across the entire supported time range, from `1677-09-21T00:12:43.145224194Z` to `2262-04-11T23:47:16.854775806Z`.

Hard deletes require every collection key field to be a tag field, except a timestamp field that is part of the key. Non-key tags do not restrict the delete, so all of the document's points are removed even if a non-key tag changed. A deletion document with an empty key tag cannot be matched to its points, so it is skipped with a warning and its points remain. Estuary selects the source operation field, or the full document when needed, to identify deletions.

Deletes run one API call per deleted document and can be slow for large delete bursts. Pending writes are completed before each delete.

## Limitations

* Changing a field's type is rejected by InfluxDB. Keep the type consistent for each field in a measurement.
* InfluxDB reserves the name `time`. A field named `time` can only be the timestamp field: it cannot be a tag, and otherwise it is excluded from points. A collection key field named `time` must be the timestamp field, unless the timestamp field projects the same location and the binding's `fields.groupBy` uses the timestamp field instead of `time`.
* Measurement names, tag keys and values, and field names cannot contain newlines or end in a backslash. Measurement names cannot start with `#`, which line protocol reads as a comment. A document with such a tag value fails the transaction. String field values may contain newlines.
* InfluxDB drops points older than the bucket's retention period. The connector keeps the other points of the batch, sets a status warning the first time points are dropped, and logs the number of dropped points for each write.
* HTTP 429 and 503 responses are retried up to five total attempts, with each wait bounded to 30 seconds and respecting `Retry-After` within that limit. Other errors fail the transaction with InfluxDB's error message.
* Bucket creation and batching configuration are not supported.
