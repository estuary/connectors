# `source-ringcentral`

`source-ringcentral` is a capture connector built with the `estuary-cdk` for capturing data from RingCentral's communications platform. This README documents non-standard / non-obvious API behavior and connector design decisions.

## Notable API Features and Behaviors

### Authentication

The connector uses **JWT authentication** for server-to-server integrations. Users must create their own private JWT Auth app in their RingCentral Developer Console:

1. **Create a JWT Auth App**: Go to https://developers.ringcentral.com/console/apps and create a new app with "JWT auth flow" as the auth type
2. **Generate a JWT Credential**: Go to https://developers.ringcentral.com/console/my-credentials, click "Create JWT", and authorize it for your app
3. **Configure the Connector**: Provide your app's Client ID, Client Secret, and the generated JWT token

The JWT is exchanged for an access token which expires after a configurable period (default 1 hour). The token exchange requires HTTP Basic Auth (client_id:client_secret) when calling the token endpoint.

> **Note**: Requiring users to create their own JWT Auth app is less user-friendly than a hosted OAuth flow. We chose this approach because RingCentral charges for registered OAuth applications, and there is not yet enough connector usage to justify the cost of Estuary creating and maintaining our own app. Once usage grows, we plan to provide an Estuary-hosted OAuth app for a simpler setup experience.

### Timestamp Filtering Precision

The RingCentral API supports **millisecond-inclusive** filtering on `dateFrom` and `dateTo` parameters. For example:
```
dateFrom=2026-01-26T14:16:23.561Z&dateTo=2026-01-26T14:16:24.000Z
```
Both boundaries are inclusive, meaning records exactly matching the boundary timestamps are included in the results.

### Call Log Filtering Limitations

The Call Log API's `dateFrom` and `dateTo` parameters filter by `startTime` (when the call started), **not** by `lastModifiedTime`. This means:

- You cannot query for "calls modified after X" via the API
- To capture updates to call records (e.g., recordings added, billing updated), you must re-fetch records by `startTime` range and compare `lastModifiedTime` client-side

### View Parameter

RingCentral API endpoints support a `view` parameter:
- `Simple`: Returns basic information
- `Detailed`: Returns full details

The connector uses `view=Detailed` for all requests. However, only a few resources (such as call logs) actually include `lastModifiedTime` in the detailed response.

### Pagination

RingCentral uses page-based pagination with `page` and `perPage` parameters:
- Maximum page size varies by endpoint (generally 100-250)
- The `navigation.nextPage` field in responses provides the URL for the next page
- Page numbers are 1-indexed

### Rate Limiting

RingCentral enforces rate limits based on "API Usage Plan Groups". Each API endpoint belongs to a group with a specific request limit per 60-second sliding window:

| Group | Requests/60s | Endpoints |
|-------|--------------|-----------|
| Heavy | 10 | User Call Log |
| Medium | 40 | Extensions, Internal Contacts |
| Light | 50 | Messages, External Contacts |

**RingCentral's rate limit penalty**: When you exceed a rate limit, RingCentral returns HTTP 429 and imposes a **60-second penalty** before allowing further requests. This is significantly worse than the natural wait time if you had stayed within limits.

The connector implements **proactive rate limiting** to avoid triggering this penalty:

1. **Tracks request timestamps** per API group in a 60-second sliding window
2. **Allows concurrent requests** up to the group's limit (enables bursting)
3. **Queues requests** when the limit is reached, waiting until the oldest request in the window expires

**Why proactive over reactive?**

- **Avoids 60-second penalties**: With proactive limiting, we wait for the oldest request to age out of the sliding window (up to 60 seconds in the worst case of a full burst). With reactive 429 handling, you waste a failed request and get penalized 60 seconds *from the point of violation*, potentially compounding delays if multiple requests hit the limit simultaneously.
- **Preserves concurrency**: Unlike a simple "sleep after each request" approach, this allows multiple requests to execute concurrently up to the limit, maximizing throughput during bursts.
- **Per-group isolation**: Different API groups are tracked independently, so heavy call log fetching doesn't impact lighter message fetching.

**Example behavior (Heavy group, 10 req/min):**
```
Requests 1-10:  Execute immediately (concurrent)
Request 11:     Waits until request 1's timestamp is >60s old
After 60s:      Window clears, can burst again
```

The `estuary-cdk` HTTP client's built-in retry logic still handles any unexpected 429 responses as a fallback.

---

## Connector Design Decisions

### Incremental Sync Strategy

#### User Call Logs

Call log records can be updated after creation (e.g., call recordings attached, billing information updated). The connector uses a two-part strategy:

1. **Cursor Tracking by `startTime`**: The cursor tracks the maximum `startTime` seen, since this is what the API can filter by.

2. **Lookback Window**: Once per day (when the cursor crosses into a new day), the connector applies a configurable lookback window (default 24 hours) to re-fetch recent call records and capture any updates. This ensures modified records are captured even though the API cannot filter by `lastModifiedTime`.

```
Day 1, 3:00 PM: cursor = 3:00 PM, fetch from 3:00 PM onwards
Day 1, 6:00 PM: cursor = 6:00 PM, fetch from 6:00 PM onwards
Day 2, 9:00 AM: cursor crossed to new day!
                Apply lookback: fetch from (6:00 PM - 24h = previous day 6:00 PM)
                This re-fetches records from the lookback window
```

The lookback window is configurable via the advanced config `lookback_window` setting (1-168 hours, default 24).

#### Messages

Messages use `creationTime` as the cursor field without a lookback window, as messages are generally not modified after creation.

### Backfill Strategy

During backfill (initial sync), the connector:

1. Sets `cutoff` to `datetime.now()` at connector startup
2. Fetches records with `startTime` between `start_date` and `cutoff`
3. **Filters out records where `lastModifiedTime >= cutoff`** - these will be captured by incremental sync instead

This prevents duplicate emissions of recently-modified records that would appear in both backfill and incremental results.

### Full Refresh Resources

Some RingCentral resources don't support incremental sync:

- **Extensions**: User and department extensions. Fetched as full refresh since the API doesn't support filtering by modification time.
- **Internal Contacts**: Corporate directory contacts (internal). The API doesn't support incremental filtering.
- **External Contacts**: User's personal address book contacts (external). The API doesn't support incremental filtering.

These resources are re-fetched completely on each sync interval (default 5 minutes).

### Resource Naming

Resources use snake_case names matching the RingCentral API concepts:
- `user_call_log` - Call records for the authenticated user's extension
- `messages` - SMS, Fax, Pager, and VoiceMail messages
- `extensions` - User and department extensions
- `internal_contacts` - Corporate directory entries (internal contacts)
- `external_contacts` - User's personal address book (external contacts)

### Binding-Aware Scope Validation

The connector validates JWT scopes based on which resources (bindings) are actually enabled, rather than requiring all scopes globally. This allows users to capture only the data they need without requesting unnecessary permissions.

**Rationale**: Different RingCentral resources require different API scopes. A user who only wants to capture messages shouldn't need to grant `ReadCallLog` permission. By validating scopes per-binding, the connector:
1. Follows the principle of least privilege
2. Allows partial deployments when full permissions aren't available
3. Provides clear error messages about which specific scope is missing for which resource

**Resource-to-Scope Mapping**:

| Resource | Required Scope | Description |
|----------|---------------|-------------|
| `extensions` | `ReadAccounts` | User and department extensions |
| `internal_contacts` | `ReadAccounts` | Corporate directory contacts |
| `external_contacts` | `ReadContacts` | User's personal address book |
| `user_call_log` | `ReadCallLog` | Call records for the authenticated user |
| `messages` | `ReadMessages` | SMS, Fax, Pager, VoiceMail |

**Validation Behavior**:
- During the `validate` RPC, the connector collects required scopes from all enabled bindings
- The connector verifies the JWT token contains all necessary scopes
- If a required scope is missing, validation fails with a clear error listing the missing scope(s)

---

## API Reference

- [RingCentral API Reference](https://developers.ringcentral.com/api-reference)
- [Call Log API](https://developers.ringcentral.com/api-reference/Call-Log)
- [Message Store API](https://developers.ringcentral.com/api-reference/Message-Store)
- [Extensions API](https://developers.ringcentral.com/api-reference/Extensions)
- [Address Book](https://developers.ringcentral.com/api-reference/address-book)
- [Authentication Guide](https://developers.ringcentral.com/guide/authentication)
