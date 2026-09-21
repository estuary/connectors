# Changelog

## 2026-08-01

### Changed
- Rewritten as a native estuary-cdk based connector, replacing the imported `tap-criteo` Singer tap and its shim. All endpoints now target Criteo API version `2026-07`.
- Authentication moved from top-level `client_id` / `client_secret` fields into a `credentials` object.
- `advertiser_ids` is now optional and scopes every resource, including `advertisers`. It was previously required and ignored.
- Statistics reports capture incrementally in date windows, with a configurable lookback for Criteo's attribution restatements, instead of re-reading all history from `start_date` on every sync.
- Entity resources are keyed by `/_meta/row_id` rather than `/id`.
- A report window that returns Criteo's 100,000-row response cap is halved and re-requested until each half fits, since a capped response may be truncated and cannot be paged. A single day still over the cap fails the capture.
- A report's `timezone` must be `UTC` or an `Area/Location` tz database name. Timezone codes and fixed UTC offsets are rejected: an offset cannot follow daylight saving, and codes such as `IST` and `CST` are ambiguous across countries.
- Report collections are named `custom_report_<name>`, keeping user-chosen report names in their own namespace so they cannot collide with a built-in resource.
- Configured `advertiser_ids` are validated against the API client's portfolio, so a mistyped ID is reported during validation instead of silently capturing nothing.
- Validation reports every problem it finds at once — unknown advertiser IDs and each broken report together — rather than one per publish attempt.
- Each configured report is validated by asking Criteo to serve one day of it, so an unaccepted dimension, metric, currency or timezone — or a combination Criteo won't serve together — is reported at publish time in Criteo's own words, rather than failing mid-capture.
- Report windows are limited to the two years of history Criteo's statistics endpoint serves. A `start_date` older than that is clamped to the earliest day Criteo answers for, and the clamp is logged.
- A report that requests a name dimension (`Campaign`) without its paired ID (`CampaignId`) has the ID added to its dimensions. Criteo returns the pair regardless, so rows are at ID granularity and keying on the name alone would collapse same-named entities.

### Removed
- The `legacy_audiences`, `legacy_campaigns`, and `legacy_categories` resources. They were served by Criteo's MAPI (`/legacy/marketing/v1`), which was deprecated in 2021 and sunset in 2022. No equivalent endpoint exists.
