---
description: Use the TikTok Ads connector to sync advertisers, campaigns, ad groups, ads, and daily or custom performance reports from the TikTok API for Business, using OAuth or access token authentication.
---

# TikTok Ads

This connector captures data from [TikTok Ads](https://ads.tiktok.com/) into Estuary collections via the [TikTok API for Business](https://business-api.tiktok.com/portal/docs) v1.3.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [Advertisers](https://business-api.tiktok.com/portal/docs?id=1739593083610113) | Full Refresh |
| [Campaigns](https://business-api.tiktok.com/portal/docs?id=1739315828649986) | Incremental |
| [AdGroups](https://business-api.tiktok.com/portal/docs?id=1739314558673922) | Incremental |
| [Ads](https://business-api.tiktok.com/portal/docs?id=1735735588640770) | Incremental |
| [ad_report_daily](https://business-api.tiktok.com/portal/docs?id=1740302848100353) | Incremental |
| [Custom reports](https://business-api.tiktok.com/portal/docs?id=1740302848100353) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding. Each entry in `/custom_reports` produces its own collection, named `custom_report_<name>`. See [Custom reports](#custom-reports).

:::tip
Campaigns, ad groups, and ads are captured in full, whatever the start date, which limits only how much report history is captured. Deletions aren't captured yet: a deleted campaign, ad group, or ad stays in its collection.
:::

## Prerequisites

To set up the TikTok Ads source connector, you'll need a [TikTok for Business account](https://ads.tiktok.com/) with access to the ad accounts you want to capture. There are two ways to authenticate:

* **OAuth**, in the Estuary web app. Unless you list Advertiser IDs, every ad account the signed-in TikTok account can access is captured, and one it gains access to later is picked up the next time the connector restarts.
* **An access token**, from your own TikTok API for Business developer app. See TikTok's [authentication guide](https://business-api.tiktok.com/portal/docs?id=1738373164380162) for how to authorize your ad accounts to the app and obtain a long-term access token. You must list the advertiser IDs to capture.

To capture a TikTok sandbox ad account instead, see [Sandbox accounts](#sandbox-accounts).

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the TikTok Ads source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication | OAuth or an access token. See the credential properties below. | object | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Report data before this date will not be replicated; campaigns, ad groups, and ads are captured in full. If left blank, the start date will be set to 30 days before the present date. | string | |
| `/advertiser_ids` | Advertiser IDs | Advertiser IDs to capture, as digits. If left empty, every advertiser the authorized TikTok account can access is captured. Required with an access token or a sandbox account. | string[] | |
| `/custom_reports` | Custom Reports | Reports to capture in addition to `ad_report_daily`. See the custom report properties below. | object[] | |
| `/advanced/is_sandbox` | Is a Sandbox Environment | Set when the credentials are for a TikTok sandbox ad account. | boolean | `false` |
| `/advanced/lookback_window_days` | Report Lookback (Days) | Days of already-captured report data to re-read on each sync, from 0 to 28. | integer | `28` |

**Credential properties for OAuth:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication | Set to `OAuth Credentials`. | string | Required |
| **`/credentials/client_id`** | Client Id | The TikTok developer app ID. Set by signing in with OAuth. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The TikTok developer app secret. Set by signing in with OAuth. | string | Required |
| **`/credentials/access_token`** | Access Token | The long-lived TikTok access token. Set by signing in with OAuth. | string | Required |

**Credential properties for Private App Credentials:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication | Set to `Private App Credentials`. | string | Required |
| **`/credentials/access_token`** | Access Token | A long-lived access token from your own TikTok developer app, or a sandbox ad account's. | string | Required |

**Custom report properties:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/custom_reports/-/name`** | Report Name | Lowercase letters, digits, and underscores, starting with a letter. The collection it is captured into is named `custom_report_<name>`. Must be unique across custom reports. | string | Required |
| `/custom_reports/-/report_type` | Report Type | `BASIC` or `AUDIENCE`. See [Custom reports](#custom-reports). | string | `BASIC` |
| `/custom_reports/-/data_level` | Level | The level rows are reported at: `AUCTION_ADVERTISER`, `AUCTION_CAMPAIGN`, `AUCTION_ADGROUP`, or `AUCTION_AD`. | string | `AUCTION_AD` |
| `/custom_reports/-/granularity` | Granularity | `daily`, `hourly`, or `lifetime`. Hourly and lifetime are available only for `BASIC` reports with no breakdowns. | string | `daily` |
| `/custom_reports/-/breakdowns` | Breakdowns | Any of `age`, `gender`, `country_code`, `language`, `platform`, `placement`, and `ac` (connection type). See [Custom reports](#custom-reports). | string[] | |
| **`/custom_reports/-/metrics`** | Metrics | A comma-separated list of up to 100 TikTok [report metrics](https://business-api.tiktok.com/portal/docs?id=1751443967255553). | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. Defaults to 30 minutes for `Campaigns`, `AdGroups`, and `Ads`, and 12 hours for `Advertisers` and reports. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-tiktok-ads-native:v1
        config:
          credentials:
            credentials_title: OAuth Credentials
            client_id: <secret>
            client_secret: <secret>
            access_token: <secret>
          start_date: 2026-01-01T00:00:00Z
          advertiser_ids:
            - "7123456789012345678"
          custom_reports:
            - name: campaign_age_gender_daily
              report_type: AUDIENCE
              data_level: AUCTION_CAMPAIGN
              granularity: daily
              breakdowns:
                - age
                - gender
              metrics: spend,impressions,clicks,conversion
    bindings:
      - resource:
          name: Campaigns
        target: ${PREFIX}/Campaigns
      - resource:
          name: ad_report_daily
        target: ${PREFIX}/ad_report_daily
      - resource:
          name: custom_report_campaign_age_gender_daily
        target: ${PREFIX}/custom_report_campaign_age_gender_daily
      {...}
```

## Custom reports

You don't list a report's ID or time dimension: its level sets the ID, and its granularity sets the time dimension.

Which metrics are valid varies by report type, level, and breakdown; see TikTok's [supported metrics](https://business-api.tiktok.com/portal/docs?id=1751443967255553). TikTok rejects an invalid metric or combination when you save the capture.

Reports include deleted campaigns, ad groups, and ads, so their past performance stays in the collection.

:::tip
TikTok caps a report request at 20,000 ads. An advertiser-level (`AUCTION_ADVERTISER`) report, or a campaign with more ads than that, captures only TikTok's newest 20,000 ads; use a campaign-level report for complete totals.
:::

## Report dates and time zones

Report dates and hours are in each advertiser's own time zone, not UTC, and are captured as TikTok sends them, such as `2026-10-12 00:00:00`.

:::tip
A day's report numbers can keep changing for up to 28 days as TikTok attributes conversions, so once a day a sync re-reads the last `/advanced/lookback_window_days` (28 by default) of every report. If your ad groups use shorter attribution windows, lower it to re-read less.
:::

## Sandbox accounts

TikTok [sandbox ad accounts](https://business-api.tiktok.com/portal/docs?id=1738855331457026) belong to a TikTok API for Business developer app. To capture from a sandbox account:

* Authenticate with an access token (`Private App Credentials`), using the sandbox access token from the app's Sandbox Ad Account page.
* List the sandbox advertiser ID in `/advertiser_ids`.
* Set `/advanced/is_sandbox` to `true`.

The sandbox allows 1 request per second, so captures are slower than in production. Advertiser-level (`AUCTION_ADVERTISER`) reports are not available in the sandbox.
