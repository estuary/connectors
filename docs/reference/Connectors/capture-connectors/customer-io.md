---
description: Use the Customer.io connector to sync message deliveries, automations, broadcasts, segments and other configuration objects, using App API Key authentication.
---

# Customer.io

This connector captures data from [Customer.io](https://customer.io) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [messages](https://docs.customer.io/integrations/api/app/tag/Messages/operation/listMessages/) | Incremental |
| [broadcasts](https://docs.customer.io/integrations/api/app/tag/Broadcasts/) | Full Refresh |
| [campaigns](https://docs.customer.io/integrations/api/app/tag/Automations/) | Full Refresh |
| [collections](https://docs.customer.io/integrations/api/app/tag/Collections/) | Full Refresh |
| [design_studio_components](https://docs.customer.io/integrations/api/app/tag/Design-Studio/) | Incremental |
| [design_studio_emails](https://docs.customer.io/integrations/api/app/tag/Design-Studio/) | Incremental |
| [design_studio_folders](https://docs.customer.io/integrations/api/app/tag/Design-Studio/) | Incremental |
| [newsletters](https://docs.customer.io/integrations/api/app/tag/Newsletters/operation/listNewsletters/) | Full Refresh |
| [object_types](https://docs.customer.io/integrations/api/app/tag/Objects/) | Full Refresh |
| [optouts](https://docs.customer.io/integrations/api/app/tag/Optouts/) | Full Refresh |
| [segments](https://docs.customer.io/integrations/api/app/tag/Segments/operation/listSegments/) | Full Refresh |
| [sender_identities](https://docs.customer.io/integrations/api/app/tag/Senders/) | Full Refresh |
| [subscription_topics](https://docs.customer.io/integrations/api/app/tag/Subscriptions/) | Full Refresh |
| [transactional_messages](https://docs.customer.io/integrations/api/app/tag/Transactional/) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

## Resource names in the Customer.io interface

Customer.io renamed several concepts in its web interface in July 2026, but left its API
names unchanged. Resource names here follow the API, so they will not always match what
you see in your workspace:

| Resource | Shown in Customer.io as |
|----------|-------------------------|
| `campaigns` | Automations |
| `newsletters` | Broadcasts > One-time sends |
| `messages` | Message activity |
| `transactional_messages` | Transactional |
| `segments` | Segments |

Note that `broadcasts` and `newsletters` are different resources despite both appearing
under Broadcasts in the interface: `broadcasts` are API-triggered, while `newsletters` are
one-time sends.

`optouts` covers SMS and WhatsApp opt-outs, which are workspace-wide. Email subscription
state is per-profile and is not part of this resource.

:::tip
The Design Studio resources are the only ones Customer.io lets you filter by when a row
last changed, so they sync incrementally rather than being re-read in full. One consequence
follows from that: because the connector only asks for rows that changed, a template
deleted in Customer.io is never reported as deleted and remains in the collection. The
full-refresh resources do detect deletions.

`design_studio_emails` captures template metadata — name, folder, timestamps — not the
rendered email body. It is an inventory of what exists, not an archive of content.

Folders are captured once, by the `design_studio_folders` resource. The emails and
components responses also carry a folder list, which the connector ignores to avoid
writing the same folder into two collections; use `parent_folder_id` to join.
:::

:::tip
Collections are a paid Customer.io feature. On a plan that does not include them,
`GET /v1/collections` succeeds but returns nothing, so the `collections` binding produces
an empty collection rather than an error. If you expect collection data and see none,
check your Customer.io plan before investigating the capture.
:::

:::tip
A delivery's engagement metrics — opens, clicks, bounces, unsubscribes — are recorded
over the days following the send, but Customer.io provides no way to query deliveries by
when those metrics changed. The connector therefore makes a second pass over deliveries
once their metrics have settled, trailing the present by `/advanced/metrics_rescan_window`
(7 days by default). Engagement recorded later than that window is not captured; raise
the value if your audience engages over a longer tail.
:::

:::tip
Customer.io serves at most six months of delivery history. If `/start_date` is older than
that, the connector begins at the six-month limit and logs a warning. Customer.io narrows
wider requests silently, so this warning is the only indication.
:::

## Prerequisites

To set up the Customer.io source connector, you'll need an [App API Key](https://docs.customer.io/integrations/api/app/).
Create one under **Account Settings > API Credentials**; you must be an Account Admin, or a
Member with the "Manage API credentials" permission. App API Keys are shown only once.

App API Keys are distinct from Track API and Pipelines API credentials, which this
connector does not use.

You will also need to know which region hosts your account. Account Admins can find this
under **Settings > Account Settings > Data and Privacy**. Using the wrong region returns an
authentication error indistinguishable from an invalid key.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Customer.io source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | App API Key | The Customer.io App API Key. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `App API Key`. | string | Required |
| **`/region`** | Region | The Customer.io data region hosting your account. One of `us` or `eu`. | string | `us` |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string | |
| `/advanced/window_size` | Window size | Time window size for each backfill and incremental request, in ISO 8601 format. ex: P1D means 1 day, PT6H means 6 hours. | string | `P1D` |
| `/advanced/metrics_rescan_window` | Metrics re-scan window | How far behind the present to trail a second pass over deliveries, re-reading them once their engagement metrics have settled. | string | `P7D` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |
| `/schedule` | Backfill schedule | The schedule for automatically rebackfilling this binding. Accepts a cron expression. For example, a schedule of `0 0 * * 0` means the binding will initiate a new backfill at 00:00 UTC every Sunday. If left empty, the binding will not automatically backfill. Applies to `messages` only. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-customer-io:v1
        config:
          credentials:
            credentials_title: App API Key
            access_token: <secret>
          region: us
          start_date: 2026-01-01T00:00:00Z
          advanced:
            metrics_rescan_window: P7D
    bindings:
      - resource:
          name: messages
        target: ${PREFIX}/messages
      - resource:
          name: campaigns
        target: ${PREFIX}/campaigns
      - resource:
          name: segments
        target: ${PREFIX}/segments
      {...}
```

## Engagement metrics

A delivery's `metrics` object holds one timestamp per metric that has been recorded. A key
that is absent means the metric has not occurred — it does not mean zero.

Alongside the named metrics, Customer.io emits one `link:<n>` and `human_link:<n>` key per
tracked link in the message body, so the set of keys varies with message content.

Each metric carries a single timestamp, so repeated opens or clicks of the same delivery by
the same recipient are not individually represented. Customer.io's reporting webhooks are
the only source of per-event resolution.

`human_opened` and `prefetch_opened` are only meaningful for deliveries sent after
2025-03-20, and `human_clicked` and `prefetch_clicked` after 2025-04-20. Earlier deliveries
may carry values for these that cannot be interpreted.

## Drafted deliveries

The `messages` resource captures sent deliveries only. Customer.io's drafts filter returns
drafted messages *instead of* sent ones rather than in addition to them, and offers no
combined view, so drafts would require a separate resource.
