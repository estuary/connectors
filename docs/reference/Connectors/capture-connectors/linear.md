---
description: Use the Linear connector to capture issues, projects, initiatives, and labels into Estuary. Authenticates with a Linear personal API key and captures each resource incrementally on its updatedAt timestamp.
---

# Linear

This connector captures issues, projects, initiatives, and labels from [Linear](https://linear.app/) into Estuary collections.
It authenticates with a Linear [personal API key](https://linear.app/developers/graphql#authentication) and reads data through the [Linear GraphQL API](https://linear.app/developers/graphql).

## Supported data resources

| Stream        | Description                                                                      | Replication             |
| ------------- | -------------------------------------------------------------------------------- | ----------------------- |
| `issues`      | Issues across every team the authenticating user can access.                     | Incremental + backfill  |
| `projects`    | Projects, including their status, dates, and progress.                           | Incremental + backfill  |
| `initiatives` | Initiatives and their owning relationships. Requires a paid Linear plan.         | Incremental + backfill  |
| `labels`      | Issue labels, both workspace-level and team-level.                               | Incremental + backfill  |

Every stream is keyed on `/id` and cursored on `updatedAt`. After the initial backfill, each
sync captures only records changed since the previous sync.

Only primary keys, cursors, and the `archivedAt` tombstone are declared on the write schema;
all remaining fields are populated by schema inference, so new Linear fields appear
automatically without a connector change.

## Archival and deletion

:::warning
Archiving a record in Linear does **not** advance its `updatedAt` timestamp. This has a
different consequence per stream, and it affects how you should interpret captured data.
:::

- **`issues` capture archival.** Linear's issue filter exposes an `archivedAt` comparator, so
  the connector runs a second pass over that field on every sync. An archived issue arrives
  with `archivedAt` set — treat that as the tombstone. Do **not** infer archival from
  `updatedAt` movement, which does not change when an issue is archived.

- **`projects`, `initiatives`, and `labels` do not capture archival.** Linear's API exposes no
  `archivedAt` filter for these types, so there is no way to detect archival incrementally. A
  record archived or deleted in Linear **remains in your destination indefinitely with a null
  `archivedAt`**, indistinguishable from a live record. Re-running a full backfill of the
  binding is the only way to reconcile.

## Rate limits

Linear meters two independent hourly budgets per user:

| Budget         | Limit                                 |
| -------------- | ------------------------------------- |
| Requests       | 2,500 per hour                        |
| Complexity     | 3,000,000 points per hour             |
| Single query   | 10,000 points (hard cap)              |

Either can bind first, because complexity is charged per record returned rather than per
request: workspaces whose records carry many populated relations spend complexity faster than
requests. The connector reads both budgets from every response and pauses until the relevant
window resets, so a healthy capture should not be rate-limited. Reducing binding `interval`s
across many bindings increases consumption of both budgets proportionally.

## Prerequisites

- A Linear account with access to the teams whose data you want to capture. The connector
  sees exactly what the authenticating user sees.
- A Linear **personal API key**. See [Authentication](#authentication) below.
- A paid Linear plan, if you intend to capture the `initiatives` stream.

## Authentication

The connector authenticates with a Linear personal API key.

To create one:

1. Sign in to Linear as the user whose access the connector should use.
2. Go to **Settings > Security & access > Personal API keys**.
3. Click **New API key**, give it a name, and grant it read access.
4. Copy the generated key immediately — Linear only displays it once.

You'll use this value as the `access_token` when configuring the connector.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Linear source connector.

### Properties

#### Endpoint

| Property                        | Title          | Description                                                                                                                                        | Type   | Required/Default |
| ------------------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/credentials`**              | Authentication | Linear API key credentials.                                                                                                                        | object | Required         |
| **`/credentials/access_token`** | API Key        | Linear personal API key, created under Settings > Security & access > Personal API keys.                                                            | string | Required         |
| `/start_date`                   | Start Date     | UTC date and time from which to start replicating data. Data generated before this date is not replicated. Defaults to 30 days before the present. | string | 30 days ago      |

#### Bindings

| Property    | Title    | Description                                                                              | Type   | Required/Default |
| ----------- | -------- | ---------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Name     | Name of the resource to capture (`issues`, `projects`, `initiatives`, or `labels`).      | string | Required         |
| `/interval` | Interval | Interval between data syncs for this resource.                                           | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-linear:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: issues
          interval: PT5M
        target: ${PREFIX}/issues
      - resource:
          name: projects
          interval: PT5M
        target: ${PREFIX}/projects
      - resource:
          name: initiatives
          interval: PT5M
        target: ${PREFIX}/initiatives
      - resource:
          name: labels
          interval: PT5M
        target: ${PREFIX}/labels
```
