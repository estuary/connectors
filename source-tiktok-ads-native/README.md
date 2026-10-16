# `source-tiktok-ads-native`

`source-tiktok-ads-native` is a capture connector built with the `estuary-cdk` for capturing data from the [TikTok API for Business](https://business-api.tiktok.com/portal/docs) v1.3. This README documents non-standard API behavior and connector design decisions. The probe behind each API behavior is in `bruno/`.

## Notable API Features and Behaviors

### Errors and Throttling Arrive as HTTP 200

TikTok answers almost every failure with HTTP 200 and a non-zero `code` in the body, so the CDK's HTTP error handling treats them as success. Throttling is reported the same way, with codes `40016`, `40100`, `40132` and `40133`, so the CDK's rate limiting never sees it. TikTok's own server errors and maintenance (`50000`, `50002`, `51305`, `60001`) arrive the same way. The connector checks `code` on every response and retries both kinds itself (see `THROTTLE_CODES` and `SERVER_ERROR_CODES`). TikTok's limits belong to the developer app, per endpoint, so every capture using Estuary's OAuth app shares them, as does the legacy connector; a customer's own app has its own. Apps start at 10 requests per second, and the owner can apply for higher levels. Since the CDK can't slow requests down for this, every request goes through one `TikTokClient`, which spaces request starts at 1 per second in the sandbox, where that limit is verified, and 10 in production. A throttle doubles the spacing for every caller and accepted requests ease it back, so captures sharing an app back off together.

### Authentication Is Not Standard OAuth

- The token exchange takes a JSON body with TikTok's own names, `app_id`, `secret` and `auth_code`, and nests the token under `data`.
- Tokens never expire, and there is no refresh token.
- Requests carry the raw token in an `Access-Token` header, not `Authorization: Bearer`.
- `oauth2/advertiser/get`, the only way to list the advertisers a token can access, authenticates with the app's `app_id` and `secret` rather than the token. A capture with a pasted access token has no app credentials, so it must list its advertiser IDs. The sandbox has no such endpoint at all.
- `advertiser/info` fails the whole request with `40001`, naming the advertiser, when one requested ID doesn't exist, rather than leaving it out of the list. `40001` is TikTok's code for any missing permission, so the connector only advises changing Advertiser IDs when the message names one.

### Entity Filters Fail Silently, Report Filters Fail Loudly

On `campaign/get`, `adgroup/get` and `ad/get`, an unknown `filtering` key is silently ignored, while an invalid value for a known key is rejected with `40002`. A misspelled filter therefore returns unfiltered data with no error. Prove an entity filter by its effect on the results, never by the request being accepted.

The report endpoint is the opposite: an unknown filter field is rejected with `40002`.

### Listings Hide Deleted Objects, and Deleted Looks Like Paused

The default entity listing leaves out deleted objects, and objects whose parent was deleted. Filtering `secondary_status` to `*_STATUS_ALL` includes them, but a deleted object then looks identical to a paused one, and the `secondary_status` field is dropped from the response even when requested in `fields`. The only way to tell which objects are deleted is a second listing filtered to `*_STATUS_DELETE`.

### Parent Deletes Cascade Without Touching the Child

Deleting a campaign does not give its ad groups `ADGROUP_STATUS_DELETE`, and does not change their `modify_time`. They move to `ADGROUP_STATUS_CAMPAIGN_DELETE` instead. Ads behave the same way, with `AD_STATUS_ADGROUP_DELETE` and `AD_STATUS_CAMPAIGN_DELETE`. A `modify_time` cursor therefore never sees a cascaded delete, though a direct delete does stamp the object's own `modify_time`.

### Listing Order and Pagination

Listings are paged with `page` and `page_size`, ordered newest ID first, with no sort parameter. If the filtered set changes while it is walked, the pages shift: an insertion only re-reads an object, but a removal skips one. A listing of live objects shrinks whenever something is deleted, so the connector only walks sets that can't shrink: the `*_STATUS_ALL` listing and the deleted listings.

### Only Ads Can Be Filtered by Modification Time

`ad/get` accepts `modified_after`. `campaign/get` and `adgroup/get` have no modification filter, so every poll of those streams lists every object of every advertiser. TikTok's SDK (`js_sdk/docs/Filtering*Get.md` in `tiktok/tiktok-business-api-sdk`) is the most reliable list of each endpoint's filter fields.

### Creation-Time Bounds Are Both Exclusive

`creation_filter_start_time` and `creation_filter_end_time` both exclude an object created at exactly the bound, so the backfill queries its start one second early. TikTok recommends spanning at most six months per request.

### Buying Types

`RESERVATION_TOP_VIEW` can't be combined with any other buying type in `filtering.buying_types`, so covering every object takes two listings (see `BUYING_TYPE_GROUPS`). A listing that includes it also rejects a `page_size` above 100.

### Reports

| Behavior | Effect on the connector |
|---|---|
| Report dates and hours are wall-clock times in the advertiser's time zone, with no offset. | See [Report Time Zones](#report-time-zones). |
| A conversion is credited to the day of the ad interaction, up to 28 days earlier, so past days keep changing. | See [Late Attribution](#late-attribution). |
| Reports leave out deleted campaigns, ad groups and ads unless a status filter asks for them, and each status filter applies at its own level only. | Every request adds a `STATUS_ALL` filter on its level's status field (see `STATUS_FILTER_FIELD`); advertiser-level reports, which have none, send no filter. |
| A request takes exactly one ID dimension and at most one time dimension. | A report's level sets its ID dimension and its granularity sets its time dimension; neither is configurable. |
| Audience reports reject `stat_time_hour` whatever the breakdown, which is stricter than TikTok's docs, and v1.3 has no lifetime audience metrics. | Audience reports are daily only. |
| An invalid metric or combination is only rejected when a report is requested, and TikTok names the bad metric. | Validation requests one row of each report, so mistakes surface when the capture is saved rather than mid-capture. |
| A request spans at most 30 days of daily rows or 1 day of hourly rows. The sandbox doesn't enforce either. | Windows are sized by `WINDOW_DAYS`. |
| Every metric arrives as a string, and a missing value as `-`. | Numeric strings become numbers and `-` becomes null. Names ending in `_id` stay strings. |
| A request covering more than 20,000 ads is silently truncated. | See [The 20,000-Ad Report Limit](#the-20000-ad-report-limit). |

### The Sandbox

The sandbox is the only free way to test, and it can't prove much:

- It allows 1 request per second per app, and returns intermittent 504s.
- It can't create ads or upload images (`51004`), so the Ads stream is always empty there.
- Its ads never deliver, so every report returns zero rows.
- It rejects advertiser-level reports with `40009`.

---

## Design Decisions

### Per-Advertiser State

Each entity and report binding captures every advertiser as its own subtask, with its own backfill and cursor. When a binding opens, an advertiser without state gets a fresh one, so an advertiser added later gets its own backfill instead of joining at the others' cursors. Advertisers are listed once when the connector starts, so an OAuth account that gains access to a new advertiser is picked up at the next restart.

### Eventual Consistency

How long TikTok's listings take to show a change has not been measured, so entity polls stop `EVENTUAL_CONSISTENCY_DELAY` (5 minutes) behind the present, and the backfill's cutoff lags by the same amount so that the two meet without a gap.

### Entity Deletes and Cascades

Campaigns, AdGroups and Ads are incremental rather than snapshots, so deletions are captured instead of silently dropped. Each poll lists the deleted IDs, then the `*_STATUS_ALL` listing, and emits every object whose `modify_time` falls in the window, marking deleted ones as deletions.

Because a cascade leaves the child's `modify_time` alone, cascaded children come from their own status listings, read only when a parent was deleted in the window. A child is emitted as deleted when its *parent's* deletion time falls in the window (see `Cascade` in `models.py`), and a changed child whose parent is in the deleted-parents listing is emitted as a deletion too.

The backfill walks creation-time windows newest first, so current objects arrive before history, back to 2016 whatever `start_date` says (see `ENTITY_HISTORY_START`): report rows reference objects of any age, so `start_date` limits report history only. It runs alongside the incremental task and skips any object modified at or after its cutoff, so a deletion made while it walks is emitted by the incremental task and never overwritten by the backfill's stale live copy.

### Report Time Zones

Report windows are computed per advertiser, in that advertiser's local dates. The zone is `display_timezone`, a named zone such as `America/Los_Angeles` that follows daylight saving, not `timezone`, a fixed offset such as `Etc/GMT+8` that is an hour off during daylight saving time. Daily rows keep their local date. Hourly rows become RFC 3339 timestamps carrying the advertiser's offset.

### Late Attribution

The first sweep of each local day re-reads from its cursor's date, less `lookback_window_days` (28 by default, TikTok's longest click attribution window), through today, and re-read rows replace the earlier ones by key. Every re-read row is emitted again, so later sweeps that day start at the cursor's date instead, as Facebook's connector does.

### The 20,000-Ad Report Limit

TikTok's synchronous report processes at most the 20,000 most recently created ads in a request. It still answers `code 0`, and the only sign is the `X-Tt-Ads-Throttle` response header. The connector reads around the limit rather than capture a partial report:

1. A report window is first requested unfiltered. Accounts under the limit make no extra requests.
2. If the truncation header appears, the connector lists every campaign of the advertiser, deleted ones included, and requests the window again in batches of up to 100 campaigns with the `campaign_ids` filter. This is TikTok's documented remedy, and 100 is its documented maximum.
3. A batch that is still truncated is split in half, down to a single campaign.

Batching by campaign is safe at the ad, ad group and campaign levels, because every row belongs to exactly one campaign, so batches neither overlap nor miss rows.

Two cases can't be split this way:

- **An advertiser-level report.** Each row covers the whole account, so per-campaign batches would produce partial totals that overwrite one another.
- **A single campaign with more than 20,000 ads.**

For these, the connector keeps the rows TikTok returns, which leave out the oldest ads, and logs a warning naming the report, advertiser and dates. Failing instead would stop the whole capture, every stream and advertiser, on every restart, over one report.

TikTok's asynchronous report API doesn't truncate, but it means creating a task, polling it and downloading a file, and it can't be exercised in the sandbox.

### Legacy Connector Parity

The legacy connector, `airbyte/source-tiktok-marketing:3.0.1`, also calls v1.3. Its default start date of 2016-09-01 is why entities backfill to 2016, and it sent no status filter on advertiser-level reports either. The docs page's migration table and metric lists were generated from its `streams.py` (`BasicReports._get_metrics`, `AudienceReport` and `NOT_AUDIENCE_METRICS`), so change them from there. Its `advertisers_audience_reports_lifetime` has no equivalent, since v1.3 has no lifetime audience metrics.

---

## Not Yet Verified in Production

The sandbox can't produce delivered ads, so these rest on TikTok's documentation until they are checked against a production account. Each has a PENDING or UNOBSERVABLE probe in `bruno/`.

- Report row shape, metric value formats, and whether report times are really in the advertiser's time zone.
- That the `campaign_ids` filter narrows a report, and that a filtered request under 20,000 ads carries no truncation header.
- Advertiser-level reports, which the sandbox rejects, and which send no status filter since TikTok documents none at that level.
- A populated ad document, and how `modified_after` treats its bound: whether it is inclusive, and whether it is really UTC as documented. The connector queries a day early and applies the window client-side, so neither can drop an ad.
- Whether production populates `secondary_status` on deleted objects.
- Whether listings without a `buying_types` filter omit reservation objects.
- The OAuth token exchange response shape (`data.access_token`).
- The rate-limit level of Estuary's TikTok app, which every OAuth capture shares. The connector starts at Basic's 10 requests per second.
- What `advertiser/info` does with an advertiser that exists but the token can't access. An ID that doesn't exist fails the whole request with `40001`, which the connector turns into advice to change Advertiser IDs.
- Whether reports include TopView ads. TikTok's report docs say TopView data needs a request filtered to the `RESERVATION_TOP_VIEW` buying type, which the connector doesn't send, so TopView spend is probably missing from reports.
- How hourly reports show the hour that repeats when daylight saving time ends. If TikTok sends two rows for it, they share a key and one overwrites the other.
- How far back TikTok serves report data. Requests before that limit, if there is one, have not been probed.

## References

TikTok's docs render client-side, but each page is also served as JSON from `https://business-api.tiktok.com/gateway/api/doc/client/node/get/v2/?doc_id=<id>`, where `<id>` is the `id` in a portal URL.

- [Run a synchronous report](https://business-api.tiktok.com/portal/docs?id=1740302848100353), including the 20,000-ad limit and the `campaign_ids` remedy
- [Authentication](https://business-api.tiktok.com/portal/docs?id=1738373164380162)
- [Get campaigns](https://business-api.tiktok.com/portal/docs?id=1739315828649986), [Get ad groups](https://business-api.tiktok.com/portal/docs?id=1739314558673922), [Get ads](https://business-api.tiktok.com/portal/docs?id=1735735588640770)
- [Sandbox accounts](https://business-api.tiktok.com/portal/docs?id=1738855331457026)
- [TikTok Business API SDK](https://github.com/tiktok/tiktok-business-api-sdk)
