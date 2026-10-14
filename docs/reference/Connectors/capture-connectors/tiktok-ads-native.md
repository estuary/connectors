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

By default, each resource is mapped to an Estuary collection through a separate binding.

`ad_report_daily` is captured without any configuration. It has one row per ad per day, with the metrics `spend`, `impressions`, `clicks`, `ctr`, `cpc`, `cpm`, `reach`, `frequency`, `conversion`, `cost_per_conversion`, `conversion_rate`, `video_play_actions`, `video_watched_2s`, `video_watched_6s`, `likes`, `comments`, `shares`, `follows`, and `profile_visits`. Each entry in `/custom_reports` produces its own collection, named `custom_report_<name>`. See [Custom reports](#custom-reports).

:::tip
Campaigns, ad groups, and ads are captured in full, whatever the start date, which limits only how much report history is captured. Deleted campaigns, ad groups, and ads are captured as deletions.

TikTok attributes a conversion to the day of the ad interaction, so a day's report numbers can keep changing for up to 28 days. Once a day, a sync re-reads the last `/advanced/lookback_window_days` (28 by default, TikTok's longest click attribution window) of every report. If all your ad groups use a shorter attribution window, you can lower this setting to re-read less data.

Reports cover every ad, even when an advertiser has more ads than the 20,000 TikTok returns per request. An advertiser-level (`AUCTION_ADVERTISER`) report, or a single campaign with more than 20,000 ads, can't be split that way: it captures TikTok's newest 20,000 ads and logs a warning, so use a campaign-level report for complete totals.
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
| **`/credentials/credentials_title`** | Authentication | Name of the credentials set. Set to `OAuth Credentials` or `Private App Credentials`. | string | Required |
| `/credentials/client_id` | Client Id | The TikTok developer app ID. Set by signing in with OAuth. | string | Required for OAuth |
| `/credentials/client_secret` | Client Secret | The TikTok developer app secret. Set by signing in with OAuth. | string | Required for OAuth |
| **`/credentials/access_token`** | Access Token | The long-lived TikTok access token. Set by signing in with OAuth, or entered directly with `Private App Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Report data before this date will not be replicated; campaigns, ad groups, and ads are captured in full. If left blank, the start date will be set to 30 days before the present date. | string | |
| `/advertiser_ids` | Advertiser IDs | Advertiser IDs to capture. If left empty, every advertiser the authorized TikTok account can access is captured. Required with an access token or a sandbox account. | string[] | |
| `/custom_reports` | Custom Reports | Reports to capture in addition to `ad_report_daily`. Each report is captured into its own collection. | object[] | |
| **`/custom_reports/-/name`** | Report Name | Lowercase letters, digits, and underscores, starting with a letter. The collection it is captured into is named `custom_report_<name>`. Must be unique across custom reports. | string | Required |
| `/custom_reports/-/report_type` | Report Type | `BASIC` or `AUDIENCE`. See [Custom reports](#custom-reports). | string | `BASIC` |
| `/custom_reports/-/data_level` | Level | The level rows are reported at: `AUCTION_ADVERTISER`, `AUCTION_CAMPAIGN`, `AUCTION_ADGROUP`, or `AUCTION_AD`. | string | `AUCTION_AD` |
| `/custom_reports/-/granularity` | Granularity | `daily`, `hourly`, or `lifetime`. | string | `daily` |
| `/custom_reports/-/breakdowns` | Breakdowns | Any of `age`, `gender`, `country_code`, `language`, `platform`, `placement`, and `ac` (connection type). See [Custom reports](#custom-reports). | string[] | |
| **`/custom_reports/-/metrics`** | Metrics | A comma-separated list of TikTok [report metrics](https://business-api.tiktok.com/portal/docs?id=1751443967255553), up to 100. | string | Required |
| `/advanced/is_sandbox` | Is a Sandbox Environment | Set when the credentials are for a TikTok sandbox ad account. | boolean | `false` |
| `/advanced/lookback_window_days` | Report Lookback (Days) | Days of already-captured report data to re-read on each sync, from 0 to 28. | integer | `28` |

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

Each custom report is keyed by `advertiser_id`, the ID of its level (`campaign_id`, `adgroup_id`, or `ad_id`), its time dimension, and its breakdowns. You don't list the ID or the time dimension: the level sets the ID, and the granularity sets the time dimension (`stat_time_day` for `daily`, `stat_time_hour` for `hourly`, and none for `lifetime`).

TikTok limits which breakdowns can be combined. The connector checks these rules when you save the configuration:

* An `AUDIENCE` report needs exactly one breakdown, or `age` and `gender` together, and must be `daily`.
* A `BASIC` report takes no breakdowns, or `country_code` alone at `daily` granularity.

Which metrics are valid varies by report type, level, and breakdown. See TikTok's [supported metrics](https://business-api.tiktok.com/portal/docs?id=1751443967255553). TikTok rejects an invalid metric or combination when you save the capture.

A `lifetime` report has one row per entity, with no date, and is re-read in full on every sync.

Reports include deleted campaigns, ad groups, and ads, so their past performance stays in the collection.

## Report dates and time zones

Report days and hours are in each advertiser's own time zone, as TikTok reports them, not UTC. `stat_time_day` is the advertiser's local date, and `stat_time_hour` is a timestamp with the advertiser's UTC offset.

## Sandbox accounts

TikTok [sandbox ad accounts](https://business-api.tiktok.com/portal/docs?id=1738855331457026) belong to a TikTok API for Business developer app. Their access tokens come from the app's Sandbox Ad Account page rather than OAuth. To capture one:

* Authenticate with an access token (`Private App Credentials`), using the sandbox access token.
* List the sandbox advertiser ID in `/advertiser_ids`.
* Set `/advanced/is_sandbox` to `true`.

The sandbox allows 1 request per second, so captures are slower than in production. Advertiser-level (`AUCTION_ADVERTISER`) reports are not available in the sandbox.

## Migrating from the TikTok Marketing connector

This connector replaces the third-party [TikTok Marketing](./tiktok.md) connector. A capture moving from it should expect these differences:

* **Collections.** Entities are captured as `Advertisers`, `Campaigns`, `AdGroups`, and `Ads`, now including deletions and every buying type. The legacy `advertiser_ids` stream is gone; `Advertisers` carries the same IDs. Of the legacy reports, only `ad_report_daily`, a smaller version of `ads_reports_daily`, is captured by default. The others can be recreated as custom reports, below.
* **Document shapes.** Report metrics and dimensions are top-level fields rather than nested under `metrics` and `dimensions`. `stat_time_day` is a date (`2026-10-12` rather than `2026-10-12 00:00:00`), `stat_time_hour` carries the advertiser's UTC offset, and numeric metrics are numbers rather than strings. In `Advertisers`, `create_time` is a date-time rather than a Unix timestamp.
* **Start date.** It defaults to 30 days before the present rather than 2016-09-01, and limits report history only. There is no end date.
* **Not available.** `advertisers_audience_reports_lifetime`, since TikTok's API v1.3 has no lifetime audience reports.

### Recreating legacy reports

To keep a legacy report, add a custom report with the settings below, one per legacy stream. Naming it after the stream keeps it recognizable, as `custom_report_<name>`.

| Legacy streams | Report Type | Level | Granularity | Breakdowns | Metrics |
|---|---|---|---|---|---|
| `advertisers_reports_daily`, `_hourly`, `_lifetime` | `BASIC` | `AUCTION_ADVERTISER` | `daily`, `hourly`, `lifetime`, as named | none | Advertiser basic, daily / hourly and lifetime |
| `advertisers_audience_reports_daily`, `_by_country_daily`, `_by_platform_daily` | `AUDIENCE` | `AUCTION_ADVERTISER` | `daily` | `age` and `gender`; `country_code`; `platform`, as named | Advertiser audience |
| `campaigns_reports_daily`, `_hourly`, `_lifetime` | `BASIC` | `AUCTION_CAMPAIGN` | `daily`, `hourly`, `lifetime`, as named | none | Campaign basic |
| `campaigns_audience_reports_daily`, `_by_country_daily`, `_by_platform_daily` | `AUDIENCE` | `AUCTION_CAMPAIGN` | `daily` | `age` and `gender`; `country_code`; `platform`, as named | Campaign audience |
| `ad_groups_reports_daily`, `_hourly`, `_lifetime` | `BASIC` | `AUCTION_ADGROUP` | `daily`, `hourly`, `lifetime`, as named | none | Ad group basic |
| `ad_group_audience_reports_daily`, `_by_country_daily`, `_by_platform_daily` | `AUDIENCE` | `AUCTION_ADGROUP` | `daily` | `age` and `gender`; `country_code`; `platform`, as named | Ad group audience |
| `ads_reports_daily`, `_hourly`, `_lifetime` | `BASIC` | `AUCTION_AD` | `daily`, `hourly`, `lifetime`, as named | none | Ad basic |
| `ads_audience_reports_daily`, `_by_country_daily`, `_by_platform_daily` | `AUDIENCE` | `AUCTION_AD` | `daily` | `age` and `gender`; `country_code`; `platform`, as named | Ad audience |

The metric lists are the legacy connector's own:

**Advertiser basic, daily** (29 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,reach,cost_per_1000_reached,frequency,video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100,profile_visits,likes,comments,shares,follows,clicks_on_music_disc,real_time_app_install,real_time_app_install_cost,app_install,cash_spend,voucher_spend
```

**Advertiser basic, hourly and lifetime** (27 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,reach,cost_per_1000_reached,frequency,video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100,profile_visits,likes,comments,shares,follows,clicks_on_music_disc,real_time_app_install,real_time_app_install_cost,app_install
```

**Advertiser audience** (6 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr
```

**Campaign basic** (28 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,reach,cost_per_1000_reached,frequency,video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100,profile_visits,likes,comments,shares,follows,clicks_on_music_disc,real_time_app_install,real_time_app_install_cost,app_install,campaign_name
```

**Campaign audience** (7 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,campaign_name
```

**Ad group basic** (51 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,reach,cost_per_1000_reached,frequency,video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100,profile_visits,likes,comments,shares,follows,clicks_on_music_disc,real_time_app_install,real_time_app_install_cost,app_install,campaign_name,campaign_id,adgroup_name,placement_type,tt_app_id,tt_app_name,mobile_app_id,promotion_type,dpa_target_audience_type,conversion,cost_per_conversion,conversion_rate,real_time_conversion,real_time_cost_per_conversion,real_time_conversion_rate,result,cost_per_result,result_rate,real_time_result,real_time_cost_per_result,real_time_result_rate,secondary_goal_result,cost_per_secondary_goal_result,secondary_goal_result_rate
```

**Ad group audience** (27 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,campaign_name,campaign_id,adgroup_name,placement_type,tt_app_id,tt_app_name,mobile_app_id,promotion_type,dpa_target_audience_type,conversion,cost_per_conversion,conversion_rate,real_time_conversion,real_time_cost_per_conversion,real_time_conversion_rate,result,cost_per_result,result_rate,real_time_result,real_time_cost_per_result,real_time_result_rate
```

**Ad basic** (54 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,reach,cost_per_1000_reached,frequency,video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100,profile_visits,likes,comments,shares,follows,clicks_on_music_disc,real_time_app_install,real_time_app_install_cost,app_install,campaign_name,campaign_id,adgroup_name,placement_type,tt_app_id,tt_app_name,mobile_app_id,promotion_type,dpa_target_audience_type,conversion,cost_per_conversion,conversion_rate,real_time_conversion,real_time_cost_per_conversion,real_time_conversion_rate,result,cost_per_result,result_rate,real_time_result,real_time_cost_per_result,real_time_result_rate,secondary_goal_result,cost_per_secondary_goal_result,secondary_goal_result_rate,adgroup_id,ad_name,ad_text
```

**Ad audience** (30 metrics):

```
spend,cpc,cpm,impressions,clicks,ctr,campaign_name,campaign_id,adgroup_name,placement_type,tt_app_id,tt_app_name,mobile_app_id,promotion_type,dpa_target_audience_type,conversion,cost_per_conversion,conversion_rate,real_time_conversion,real_time_cost_per_conversion,real_time_conversion_rate,result,cost_per_result,result_rate,real_time_result,real_time_cost_per_result,real_time_result_rate,adgroup_id,ad_name,ad_text
```
