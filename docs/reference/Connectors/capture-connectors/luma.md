---
description: Use the Luma connector to sync your calendar, its events, every event's guests, and membership tiers into Estuary using a Luma calendar API key.
---

# Luma

This connector captures data from [Luma](https://luma.com) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [calendars](https://docs.luma.com/reference/get_v1-calendars-get) | Full Refresh |
| [events](https://docs.luma.com/reference/get_v1-calendars-events-list) | Full Refresh |
| [guests](https://docs.luma.com/reference/get_v1-events-guests-list) | Full Refresh |
| [membership_tiers](https://docs.luma.com/reference/get_v1-memberships-tiers-list) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
Luma's API exposes no modification timestamps and no "changed since" filters, so every resource is re-listed in full on each poll. New, updated, and deleted records are all picked up this way; the `interval` on each binding controls how quickly. The `guests` stream lists guests one event at a time, so each poll costs one request per event on the calendar plus the event listing itself. Calendar API keys allow 200 requests per minute; the default 15 minute interval comfortably covers calendars with up to a few thousand events.
:::

A Luma API key is scoped to a single calendar, so the `calendars` stream contains exactly one document: the calendar the key was created on. The `events` stream includes events the calendar manages across all submission statuses (`approved` and `pending`) and both Luma-hosted and external events. Events that are merely listed on the calendar but managed by another calendar are not captured.

## Prerequisites

To set up the Luma source connector, you'll need:

* A Luma calendar with an active [Luma Plus](https://help.luma.com/p/luma-api) subscription.
* A [calendar API key](https://docs.luma.com/reference/getting-started-with-your-api), generated from **Calendar > Settings > API keys** (`luma.com/calendar/manage/api-keys`). The key grants full access to the calendar it was created on, so store it as a secret.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Luma source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | Luma calendar API key. Requires a Luma Plus subscription on the calendar. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `API Key`. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-luma:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
    bindings:
      - resource:
          name: calendars
          interval: PT1H
        target: ${PREFIX}/calendars
      - resource:
          name: events
          interval: PT5M
        target: ${PREFIX}/events
      - resource:
          name: guests
          interval: PT15M
        target: ${PREFIX}/guests
      - resource:
          name: membership_tiers
          interval: PT1H
        target: ${PREFIX}/membership_tiers
```

## Guest documents

Luma's guest listing does not include the event a guest belongs to, so the connector adds an `event_id` field to every `guests` document from the request that produced it. Use `event_id` to join guests to `events`.
