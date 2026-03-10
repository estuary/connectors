# Source Gong

Estuary CDK connector for [Gong](https://www.gong.io/) revenue intelligence platform.

## Streams

| Stream | Endpoint | Method | Sync Mode | Cursor Field |
|--------|----------|--------|-----------|--------------|
| `users` | `/v2/users` | GET | Full refresh | - |
| `scorecard_definitions` | `/v2/settings/scorecards` | GET | Full refresh | - |
| `calls` | `/v2/calls` | GET | Incremental | `started` |
| `users_extensive` | `/v2/users/extensive` | POST | Incremental | `created` |
| `scorecards` | `/v2/stats/activity/scorecards` | POST | Incremental | `reviewTime` |

## API Constraints

### Rate Limits

- **3 requests/second** (proactively limited to 170/60s for safety)
- **10,000 requests/day**
- Returns HTTP 429 with `Retry-After` header when exceeded

### Date/Time Formats

| Endpoint | Format | Example |
|----------|--------|---------|
| `/v2/calls` | ISO-8601 | `2024-01-15T08:00:00Z` |
| `/v2/users/extensive` | ISO-8601 | `2024-01-15T08:00:00Z` |
| `/v2/stats/activity/scorecards` | **YYYY-MM-DD** | `2024-01-15` |

### Filter Parameter Behavior

- `from*` parameters: **Inclusive** (>=)
- `to*` parameters: **Exclusive** (<)

### Pagination

All endpoints use cursor-based pagination:
- Response: `{"records": {"cursor": "..."}}`
- Request: `cursor` query param (GET) or body field (POST)

## Design Decisions

### Proactive Rate Limiting

Uses sliding window algorithm (170 req/60s) rather than reactive 429 handling to:
- Avoid wasting daily quota on rejected requests
- Ensure predictable throughput

### Incremental Cursor Storage

Cursors stored as unix timestamps (int) for consistency across streams, even though API uses ISO-8601 strings.

### 1-Day Fetch Windows

Incremental `fetch_changes` limits queries to 1-day windows to prevent timeouts on large date ranges.

## Authentication

Basic Auth with Access Key and Access Key Secret from Gong API settings.

```
Authorization: Basic base64(accessKey:accessKeySecret)
```

## References

- [Gong API Documentation](https://gong.app.gong.io/settings/api/documentation)
- [Gong API Help](https://help.gong.io/docs/what-the-gong-api-provides)
