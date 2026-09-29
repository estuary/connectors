---
description: Use the Aircall connector to sync calls, contacts, users, teams, numbers, tags, webhooks, and company data, using API ID and API token authentication with incremental call and contact capture.
---

# Aircall

This connector captures data from Aircall into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [calls](https://developers.aircall.io/api-references#list-all-calls) | Incremental |
| [company](https://developers.aircall.io/api-references#retrieve-company) | Full Refresh |
| [contacts](https://developers.aircall.io/api-references#list-all-contacts) | Incremental |
| [numbers](https://developers.aircall.io/api-references#list-all-numbers) | Full Refresh |
| [tags](https://developers.aircall.io/api-references#list-all-tags) | Full Refresh |
| [teams](https://developers.aircall.io/api-references#list-all-teams) | Full Refresh |
| [user_availability](https://developers.aircall.io/api-references#retrieve-list-of-users-availability) | Full Refresh |
| [users](https://developers.aircall.io/api-references#list-all-users-v2) | Full Refresh |
| [webhooks](https://developers.aircall.io/api-references#list-all-webhooks) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
Aircall limits every list request to 10,000 results. The connector requests `calls` in time windows that each stay under that limit, so accounts with any number of calls can be captured in full.

Calls keep changing after they start: they are ended, tagged, commented, assigned, and gain recordings. The connector re-reads each call 24 hours after it started to capture those changes. Changes made more than 24 hours later are only captured by backfilling the `calls` binding.

Aircall only serves the last six months of calls through its API, so `calls` is backfilled from the later of the configured start date and roughly six months ago. The `recording` and `voicemail` links on calls are signed URLs that expire within a few hours of being captured.
:::

:::tip
The `contacts` stream re-captures all contacts with periodic backfills. These backfills can be scheduled with the `schedule` resource config setting. By default, `contacts`'s schedule is `0 0 * * *`, which means the stream attempts to backfill every day at 00:00 UTC. If more than 10,000 contacts change between two polls, the most recent 10,000 are captured right away and the rest are captured by the next scheduled backfill.

Aircall doesn't expose deleted contacts, so contacts deleted in Aircall remain in the collection. Only shared contacts are available through the Aircall API; users' personal contacts are not captured.
:::

## Prerequisites

To set up the Aircall source connector, you'll need an Aircall [API ID and API token](https://dashboard.aircall.io/integrations/api-keys). Creating API keys requires an Aircall admin account.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Aircall source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/api_id`** | API ID | The Aircall API ID. | string | Required |
| **`/credentials/api_token`** | API Token | The Aircall API token. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `API Token`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Calls started before this date will not be replicated. Aircall only serves the last six months of calls, so earlier dates are clamped to that horizon. Other streams capture all of their data regardless of this date. If left blank, the start date will be set to 30 days before the present date. | string | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |
| `/schedule` | Backfill schedule | The schedule for automatically backfilling this binding. Accepts a cron expression. For example, a schedule of `0 0 * * *` means the binding will initiate a new backfill at 00:00 UTC every day. If left empty, the binding will not automatically backfill. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-aircall-native:v1
        config:
          credentials:
            credentials_title: API Token
            api_id: <secret>
            api_token: <secret>
          start_date: 2026-04-01T00:00:00Z
    bindings:
      - resource:
          name: calls
        target: ${PREFIX}/calls
      - resource:
          name: contacts
          schedule: 0 0 * * *
        target: ${PREFIX}/contacts
      - resource:
          name: users
        target: ${PREFIX}/users
      {...}
```

## Webhook signing tokens

The `webhooks` stream omits each webhook's `token`, which is the secret Aircall uses to sign the events it sends to that webhook.
