# Changelog

## 2026-09-24

### Added

- New `Sessions` binding, capturing one document per visit to your site or app:
  entry and exit URLs and pathnames, the full UTM and click-ID attribution set,
  pageview, autocapture and screen counts, unique URL count, `is_bounce`
  (`0` or `1`, not a boolean), channel type, session duration and web vitals.

  `session_id` joins to `$session_id` on `Events`, and `distinct_id` to
  `distinct_id` on `Events`, so sessions can be used to attribute events to an
  acquisition channel. There is no join to `Persons`: PostHog maps distinct ids
  to people in a table this connector does not capture.

  A session is not final when it first appears. PostHog recalculates it as more
  events arrive, so **the same `session_id` is captured several times** and its
  counts, duration, exit URL and `is_bounce` change until the visit settles —
  within 30 minutes of the last activity in most cases. Reduction keeps the
  latest values, but queries run against an in-flight session may see partial
  results.

  Events that reach PostHog more than 3 days after their session ended are not
  reflected.

  Requires the `query:read` scope on your Personal API Key.
