Flow GA4 BigQuery Source Connector
==================================

Captures Google Analytics 4 BigQuery exports into Flow. Targets the daily batch
export tables produced by GA4's BigQuery linkage:

- `events_YYYYMMDD`
- `users_YYYYMMDD`
- `pseudonymous_users_YYYYMMDD`

The connector polls daily by default. Each poll considers a trailing live
window of recent daily tables (4 by default, configurable via `window_days`)
and decides what to capture:

- Tables outside the window are captured exactly once and never re-queried.
- The newest table in the window is primary-captured to land fresh data
  quickly.
- The oldest table in the window is final-captured when it rotates out, to
  pick up any late-arriving events GA4 wrote into prior dates' tables (GA4
  backfills late events for up to 72 hours).
- Intermediate tables in the window are skipped by default. Set
  `capture_intermediate=true` to query them on every poll instead, trading
  BigQuery scan cost for fresher intermediate-day data.

Each table is queried individually so mid-poll crashes resume from the
within-table cursor rather than restarting the live window.

Useful commands:

    docker build -t ghcr.io/estuary/source-ga4-bigquery:local -f source-ga4-bigquery/Dockerfile .
    flowctl raw discover --source acmeCo/flow.yaml
    flowctl raw capture acmeCo/flow.yaml

Example `flow.yaml`:

    captures:
      acmeCo/source-ga4-bigquery:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-ga4-bigquery:local"
            config:
              project_id: my-gcp-project
              credentials:
                auth_type: CredentialsJSON
                credentials_json: "<REDACTED>"
              dataset: analytics_1234
        bindings: []

Limitations
-----------

- Streaming intraday tables (`events_intraday_*`) are not captured.
- GA360 Fresh Daily tables (`events_fresh_*`) are not captured.
- The first poll backfills every dataset table that's not excluded by
  `min_date`, which can be many TB scanned for high-traffic properties.
  Operators should price this out before enabling the connector and can
  use `min_date` to bound the cost.
- Tables that GA4 modifies after they have rotated out of the live window
  are not re-captured.
- Late-arriving events past the live window (4 days by default) are not
  re-captured. GA4 docs cap meaningful late arrivals at 72 hours so the
  default behavior matches this.
