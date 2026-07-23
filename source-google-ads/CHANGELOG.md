# Changelog

## 2026-07-22

### Changed
- Upgraded to Google Ads API v25 from v21.
- Renamed video metrics on `campaigns` and the performance-report streams: `metrics.average_cpv` → `metrics.trueview_average_cpv`, `metrics.video_view_rate` → `metrics.video_trueview_view_rate`, `metrics.video_views` → `metrics.video_trueview_views`. These now cover TrueView formats only, so values may differ for non-TrueView video.
- Renamed `campaign.start_date` → `campaign.start_date_time` and `campaign.end_date` → `campaign.end_date_time` on the `campaigns` stream; values now include a time component.
- Added `customer.video_brand_safety_suitability` to the `accounts` stream, replacing the account-level signal removed from `campaign`.

### Removed
- Dropped the `ad_group_ad.ad.call_ad.*` fields from `ad_group_ads` — Google removed Call ads in v23.
- Dropped `campaign.video_brand_safety_suitability` from `campaigns` — Google removed it at the campaign level in v24.
