# source-claude-api

Estuary Flow capture connector for the **Anthropic Claude Admin API**.

## Streams

| Stream | Endpoint | Mode | Key |
|---|---|---|---|
| `Organization` | `GET /v1/organizations/me` | Full-refresh snapshot (singleton) | `/id` |
| `Users` | `GET /v1/organizations/users` | Full-refresh snapshot (ID-cursor paginated) | `/id` |
| `ClaudeCodeUsageReport` | `GET /v1/organizations/usage_report/claude_code` | Incremental, day-by-day | `/date`, `/organization_id`, `/actor_id` |

## Authentication

Requires an **Admin API key** (`sk-ant-admin…`), provisionable only by an organization
admin in the Claude Console. It is sent as the `x-api-key` header; every request also
sends `anthropic-version: 2023-06-01`.

## Configuration

| Field | Required | Default | Notes |
|---|---|---|---|
| `admin_api_key` | yes | — | secret; `sk-ant-admin…` |
| `advanced.base_url` | no | `https://api.anthropic.com` | |
| `advanced.start_date` | no | 7 days ago | UTC backfill start for the Claude Code usage report only |

## Notes on the Claude Code usage report

- The API returns data for a **single UTC day** per request (`starting_at`, `YYYY-MM-DD`);
  there is no date-range query. The connector iterates one day at a time, paginating
  within each day via the opaque `page`/`next_page` cursor, and checkpoints after each
  day is fully drained.
- Data has a **~1 hour freshness lag** (only data older than ~1h is returned). The
  connector waits until a day is fully complete plus a safety margin before capturing
  it, so late-arriving rows are not skipped.
- Granularity is one record per actor (user or API key) per day.

## Development

```bash
poetry install
cp config.yaml.template config.yaml   # fill in your sk-ant-admin… key
poetry run pytest                      # spec tests run without creds; discover needs config.yaml

# With flowctl:
flowctl raw spec --source test.flow.yaml
flowctl raw discover --source test.flow.yaml      # regenerates acmeCo/ collection schemas
flowctl preview --source test.flow.yaml --sessions 1
```

`config.yaml`, `.env`, and `test.sh` hold secrets and are gitignored.
