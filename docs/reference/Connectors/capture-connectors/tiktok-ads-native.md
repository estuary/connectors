---
description: Use the TikTok Ads connector to sync advertisers, campaigns, ad groups, ads, creative assets, and prebuilt or custom performance reports from the TikTok Business API.
---

# TikTok Ads

This connector captures data from [TikTok Ads](https://ads.tiktok.com/) into Estuary collections
via the [TikTok Business API](https://business-api.tiktok.com/portal/docs) v1.3.

[`ghcr.io/estuary/source-tiktok-ads-native:dev`](https://ghcr.io/estuary/source-tiktok-ads-native:dev)
provides the latest connector image. You can also follow the link in your browser to see past
image versions.

## Supported data resources

The following entity resources are captured:

| Resource | API reference | Replication Mode |
|---|---|---|
| `advertisers` | [Get ad account details](https://business-api.tiktok.com/portal/docs?id=1739593083610113) | Snapshot |
| `locations` | [Get ad delivery locations](https://business-api.tiktok.com/portal/docs?id=1737476645761026) | Snapshot |
| `campaigns` | [Get campaigns](https://business-api.tiktok.com/portal/docs?id=1739315828649986) | Incremental |
| `ad_groups` | [Get ad groups](https://business-api.tiktok.com/portal/docs?id=1739314558673922) | Incremental |
| `ads` | [Get ads](https://business-api.tiktok.com/portal/docs?id=1735735588640770) | Incremental |
| `videos` | [Get video assets](https://business-api.tiktok.com/portal/docs?id=1740050472224769) | Incremental |
| `images` | [Get image assets](https://business-api.tiktok.com/portal/docs?id=1740052016789506) | Incremental |

The following prebuilt report resources are captured, all via
[Run a synchronous report](https://business-api.tiktok.com/portal/docs/run-a-synchronous-report/v1.3):

| Resource | Level | Aggregation | Breakdown |
|---|---|---|---|
| `campaign_report_daily` | Campaign | Daily | — |
| `campaign_report_hourly` | Campaign | Hourly | — |
| `campaign_report_lifetime` | Campaign | Lifetime | — |
| `adgroup_report_daily` | Ad group | Daily | — |
| `adgroup_report_hourly` | Ad group | Hourly | — |
| `adgroup_report_lifetime` | Ad group | Lifetime | — |
| `ad_report_daily` | Ad | Daily | — |
| `ad_report_hourly` | Ad | Hourly | — |
| `ad_report_lifetime` | Ad | Lifetime | — |
| `campaign_age_gender_report` | Campaign | Daily | Age and gender |
| `campaign_country_report` | Campaign | Daily | Country |
| `campaign_language_report` | Campaign | Daily | Language |
| `campaign_platform_report` | Campaign | Daily | Platform |
| `ad_age_gender_report` | Ad | Daily | Age and gender |
| `ad_country_report` | Ad | Daily | Country |
| `ad_language_report` | Ad | Daily | Language |
| `ad_platform_report` | Ad | Daily | Platform |
| `gmv_advertiser_country_report_daily` | Advertiser | Daily | Country |
| `gmv_campaign_country_report_daily` | Campaign | Daily | Country |

Additional reports can be defined with the **Custom Reports** configuration field, described
below.

:::tip
Report metrics are not final when first read. TikTok attributes a conversion to the day of
the **ad interaction**, not the day the conversion happened, so a conversion occurring weeks
later rewrites the report row for the original day. The connector therefore re-reads a
trailing window of already-captured days on every sync, sized by the **Lookback Window**
setting.

The default is 7 days, which matches TikTok's own default click attribution window. If any
of your ad groups use a longer click attribution window — TikTok allows up to **28 days** —
raise this setting to match, or those late conversions will never be captured. You can read
the configured window per ad group from the `ad_groups` resource.
:::

:::tip
Reports covering more than **20,000 ads** are silently truncated by TikTok to the most
recently created 20,000, and the response still reports success. The connector logs a warning
whenever this happens. If you see it, narrow the affected binding — for example by splitting
the capture across advertiser accounts — because the omitted ads are the oldest ones and
which ads are dropped changes over time.
:::

## Prerequisites

* A TikTok Ads Business account with permission to access the advertiser accounts you want
  to capture.
* A [TikTok for Business developer app](https://business-api.tiktok.com/portal), if you are
  authenticating with an access token rather than OAuth.

TikTok Marketing API access tokens do not expire, but they become invalid if the advertiser
revokes the authorization.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog
specification file. See [connectors](/concepts/connectors.md#using-connectors) to learn more
about using connectors. The values and specification sample below provide configuration
details specific to the TikTok Ads source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication | Authentication details. | object | Required |
| **`/credentials/credentials_title`** | Authentication Type | Either `OAuth Credentials` or `Private App Credentials`. | string | Required |
| `/credentials/client_id` | App ID | Your TikTok developer app ID. OAuth only. | string | |
| `/credentials/client_secret` | Secret | Your TikTok developer app secret. OAuth only. | string | |
| **`/credentials/access_token`** | Access Token | Your long-lived TikTok access token. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Data generated before this date will not be replicated. | string | 30 days before the present |
| `/advertiser_ids` | Advertiser IDs | Advertiser accounts to capture. Leave empty to capture every accessible account. Required when authenticating with an access token. | array | `[]` |
| `/custom_reports` | Custom Reports | A JSON array describing additional reports to sync. | string | `""` |
| `/advanced/lookback_window_days` | Lookback Window (days) | Days of already-captured report data to re-read on each sync. | integer | `7` |
| `/advanced/is_sandbox` | Use Sandbox | Read from a TikTok sandbox ad account instead of production. | boolean | `false` |

When authenticating with an access token, `advertiser_ids` is **required**. Discovering
accounts automatically calls an endpoint that authenticates with the app ID and secret, which
an access token alone does not carry.

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of this resource. | string | Required |
| `/interval` | Interval | Interval between updates for this resource. | string | Varies by resource |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-tiktok-ads-native:dev
        config:
          credentials:
            credentials_title: OAuth Credentials
            client_id: {secret}
            client_secret: {secret}
            access_token: {secret}
          start_date: "2024-01-01T00:00:00Z"
          advanced:
            lookback_window_days: 28
    bindings:
      - resource:
          name: campaigns
        target: ${PREFIX}/campaigns
      - resource:
          name: ad_report_hourly
        target: ${PREFIX}/ad_report_hourly
```

## Custom reports

The **Custom Reports** field takes a JSON array. Each entry defines one additional report
stream:

```json
[
  {
    "name": "adgroup_country_report",
    "report_type": "AUDIENCE",
    "data_level": "AUCTION_ADGROUP",
    "granularity": "daily",
    "dimensions": ["country_code"],
    "metrics": ["spend", "impressions", "clicks", "conversion"]
  }
]
```

| Field | Description |
|---|---|
| `name` | The stream name. Must be unique across all reports, including the prebuilt ones. |
| `report_type` | `BASIC`, `AUDIENCE`, or `TT_SHOP`. Defaults to `BASIC`. |
| `data_level` | `AUCTION_ADVERTISER`, `AUCTION_CAMPAIGN`, `AUCTION_ADGROUP`, or `AUCTION_AD`. |
| `granularity` | `daily`, `hourly`, or `lifetime`. |
| `dimensions` | Breakdown dimensions only. |
| `metrics` | The metrics to request. At most 100. |

You do not list the ID or time dimension. The ID dimension follows from `data_level` and the
time dimension from `granularity`, which is what keeps each request within TikTok's rule of
one ID dimension and at most one time dimension.

TikTok constrains which dimensions can be combined, and rejects an invalid combination with a
generic error. The connector checks these rules when you save the configuration instead:

* A **basic** report accepts at most one breakdown dimension.
* An **audience** report requires exactly one audience dimension. `age` and `gender` are the
  only pair that may be requested together.
* `interest_category`, `interest_category_tier2`, `interest_category_tier3`,
  `interest_category_tier4`, and `device_brand_id` have no time series, so they require
  `lifetime` granularity.
* `country_code` cannot be combined with `hourly` granularity.
* `ad_type` cannot be combined with the advertiser report level.

Which metrics are valid also varies by report type, level, granularity, and dimension. Consult
TikTok's [supported metrics](https://business-api.tiktok.com/portal/docs?id=1751443967255553)
reference when building a custom report.

## Timezones and report dates

Report dates are expressed in each ad account's own timezone, not UTC, so the connector reads
every advertiser on its own calendar. Two advertisers in different timezones will therefore be
current to different wall-clock instants at any given moment, which is expected.

## Sandbox accounts

Setting **Use Sandbox** points the connector at `https://sandbox-ads.tiktok.com` instead of
the production host. Sandbox ad accounts are created from the TikTok developer portal at no
cost and without approval, and are useful for exercising the connector before a production
account is authorized.

Sandbox accounts differ from production in ways that affect what a capture returns:

* Their access tokens are not valid against production, and production tokens are not valid
  against the sandbox.
* They are issued directly rather than through the OAuth flow, so use the access token
  authentication method and list the sandbox advertiser ID explicitly.
* Mock report data covers only **2020-12-08 through 2020-12-19**, so set the start date
  accordingly or reports will return nothing.
* Rate limits are far tighter: 1 request per second, 30 per minute, and 1,000 per day. Enable
  only the bindings you need, or a single backfill will exhaust the daily allowance.
* Asynchronous reports and result ordering are unsupported, and some metrics return `0` or a
  null value regardless of the mock data.

## Reservation reports

TikTok removed the Reservation report type from the API, and recommends auction reports
instead, which include data for reservation ad accounts. This connector does not offer
reservation report levels.
