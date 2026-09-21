---
description: Use the Criteo connector to sync advertisers, ad sets, audiences, campaigns, and custom statistics reports, using OAuth2 client credentials authentication.
---

# Criteo

This connector captures data from Criteo's [Marketing Solutions API](https://developers.criteo.com/marketing-solutions/reference) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [ad_sets](https://developers.criteo.com/marketing-solutions/reference/searchadsets) | Full Refresh |
| [advertisers](https://developers.criteo.com/marketing-solutions/reference/getportfolio) | Full Refresh |
| [audiences](https://developers.criteo.com/marketing-solutions/reference/searchaudiencesv1) | Full Refresh |
| [campaigns](https://developers.criteo.com/marketing-solutions/reference/searchcampaigns) | Full Refresh |
| [Statistics reports](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics) | Incremental |

Each entry in the `reports` endpoint configuration produces its own collection, named `custom_report_<name>` and keyed by that report's dimensions.

:::tip
Criteo restates statistics as attribution windows close, so a day's numbers keep changing after that day ends. Each report sweep therefore rewinds `/advanced/report_lookback_days` (30 by default, the widest standard Criteo attribution window) behind its cursor and re-queries that tail rather than resuming exactly where it stopped.

Criteo's statistics endpoint serves at most two years of history. A `start_date` older than that is clamped to the earliest day for which Criteo returns statistics.

Criteo also caps a single report response at 100,000 rows, with no way to page past it. A window that comes back at that cap may have been truncated, so the connector halves it and re-requests each half until every one fits, rather than risking a silently truncated result. Lowering `/advanced/report_window_size` avoids the wasted round trips. If a *single day* still exceeds the cap there is nothing left to narrow and the capture fails — request fewer dimensions, or split the report into several reports scoped to different `advertiser_ids`.
:::

## Prerequisites

To set up the Criteo source connector, you'll need:

* A Criteo API [client ID and client secret](https://developers.criteo.com/marketing-solutions/docs/how-to-get-your-api-credentials) with Marketing Solutions access.

Optionally, you can restrict the capture to specific [advertiser IDs](https://developers.criteo.com/marketing-solutions/docs/get-advertiser-portfolio). If none are configured, every advertiser in the API client's portfolio is captured.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Criteo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/client_id`** | Client Id | The Criteo API client ID. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Criteo API client secret. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `OAuth Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Report data before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. Criteo's statistics reports serve at most two years of history, so an earlier start date is clamped to that. | string | |
| `/advertiser_ids` | Advertiser IDs | Advertiser IDs to capture, applied to every resource. If left empty, every advertiser in the API client's portfolio is captured. | string[] | |
| `/reports` | Reports | Statistics reports to capture. Each report is captured into its own collection, keyed by the report's dimensions. | object[] | |
| `/reports/-/name` | Report Name | Name of this report. The collection it is captured into is named `custom_report_<name>`. Must be unique across reports. | string | Required |
| `/reports/-/grain` | Time Grain | Time granularity of the report, `Day` or `Hour`. | string | `Day` |
| `/reports/-/dimensions` | Additional Dimensions | Criteo statistics [dimensions](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics#dimensions) to group the report by beyond its time grain. These join the grain to form the collection's key. Time dimensions are not accepted here. | string[] | |
| `/reports/-/metrics` | Metrics | Criteo statistics [metrics](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics#full-list-of-metrics) to report on. | string[] | Required |
| `/reports/-/currency` | Currency | ISO 4217 currency code (three capital letters) that monetary metrics are reported in. | string | `USD` |
| `/reports/-/timezone` | Timezone | Timezone the report's date dimensions are computed in, as `UTC` or an `Area/Location` tz database name such as `America/New_York`. See below. | string | `UTC` |
| `/advanced/report_window_size` | Report Window Size (Days) | Number of days requested per statistics report call. | integer | `4` |
| `/advanced/report_lookback_days` | Report Lookback (Days) | Number of days before the cursor that are re-queried on every sweep. Minimum 1. | integer | `30` |

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
        image: ghcr.io/estuary/source-criteo:v2
        config:
          credentials:
            credentials_title: OAuth Credentials
            client_id: <secret>
            client_secret: <secret>
          start_date: 2025-01-01T00:00:00Z
          advertiser_ids:
            - "12345"
            - "67890"
          reports:
            - name: DailyCampaignPerformance
              currency: GBP
              grain: Day
              dimensions:
                - CampaignId
              metrics:
                - AdvertiserCost
                - Clicks
                - Displays
                - Visits
    bindings:
      - resource:
          name: advertisers
        target: ${PREFIX}/advertisers
      - resource:
          name: campaigns
        target: ${PREFIX}/campaigns
      - resource:
          name: custom_report_DailyCampaignPerformance
        target: ${PREFIX}/custom_report_DailyCampaignPerformance
      {...}
```

## Report time grain

Every report has a `grain` of either `Day` (the default) or `Hour`, set separately from its other dimensions. Reports are fetched in multi-day windows and the dimensions form the collection key, so each row has to carry a timestamp at least as fine as a day — otherwise every window would return one aggregate row per key and each window would overwrite the previous one rather than accumulating.

Because the grain is its own field, time dimensions are not accepted under `dimensions`; set `grain` instead. To aggregate over a longer period, capture at `Day` grain and roll up downstream with a derivation or in your destination.

Criteo also returns the counterpart of every ID/name dimension pair — `AdsetId`/`Adset`, `CampaignId`/`Campaign`, `AdvertiserId`/`Advertiser`, and so on — whether or not you asked for it, so rows are always at ID granularity. If you request only the name, the connector adds the paired ID to the report's dimensions so the collection key matches that granularity. Otherwise two same-named campaigns would collapse onto one key. The added dimension is visible in the discovered collection key.
