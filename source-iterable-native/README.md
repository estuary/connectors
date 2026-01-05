# `source-iterable-native`

`source-iterable-native` is a capture connector built with the `estuary-cdk` for capturing data from Iterable. This README documents non-standard API behavior and connector design decisions.

## Notable API Features and Behaviors

### Malformed Datetime Strings

Iterable returns datetime strings in non-ISO 8601 formats. The connector normalizes these before validation:

| Input | Normalized |
|-------|------------|
| `2026-01-14 06:20:22 +00:00` | `2026-01-14T06:20:22+00:00` |
| `2025-12-19T22:43:34.134105.000Z` | `2025-12-19T22:43:34.134105Z` |

Fixing these datetimes allows Flow's schema inference to infer datetime formats for these fields.

### Rate Limits

Some endpoints have different rate limits that aren't (currently) managed by the CDK's standard rate limiting:

| Endpoint | Rate Limit |
|----------|------------|
| `/lists/getUsers` | 5 requests/minute |
| `/campaigns/metrics` | 10 requests/minute |

The connector manually sleeps between requests to these endpoints to avoid hitting rate limits.

---

## Design Decisions

### Project Types and User Identification

The configured [project type](https://support.iterable.com/hc/en-us/articles/9216719179796-Project-Types-and-Unique-Identifiers) determines the `users` stream primary key:

| Project Type | Primary Key |
|--------------|-------------|
| Email-based | `email` |
| UserID-based | `itblUserId` |
| Hybrid | `itblUserId` |

### Synthetic IDs for Events

Events lack a natural unique identifier, and the identifying fields vary by event type. The connector computes a synthetic ID (`_estuary_id`) by hashing `createdAt`, `email`, `itblUserId`, `campaignId`, `eventName`, and `eventType`.

### Eventual Consistency and Lookback Windows

Iterable's export data is eventually consistent. The connector uses a dual-cursor strategy: a realtime cursor at the current position, and a lookback cursor that trails behind it (see `EVENTUAL_CONSISTENCY_LAG`). Data missed during the realtime pass gets captured during the lookback pass.

#### Offline Events

Iterable's mobile SDKs queue events when offline, sending them later with two timestamps: `createdAt` (when triggered) and `sentAt` (when sent). It is unclear whether Iterable's export API filters by `createdAt` alone or by `max(createdAt, sentAt)`.

Reference: https://support.iterable.com/hc/en-us/articles/360035395671-Tracking-Events-and-Purchases-with-Iterable-s-Mobile-SDKs#offline-events-processing

### Campaign Metrics Filtering

The connector conditionally fetches metrics for campaigns depending on the campaign's state:

- **Pre-launch campaigns** (Draft, Recurring, Scheduled): Skipped (no metrics)
- **In-progress campaigns** (Ready, Running, Recalling): Always fetched
- **Final state campaigns** (Aborted, Archived, Finished, Recalled): Fetched if the campaign ended within 15 days to capture late-arriving attributions
