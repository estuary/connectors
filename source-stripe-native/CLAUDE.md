# source-stripe-native

Capture connector for Stripe's REST API. Most streams follow a list+events polling pattern:
the connector backfills via `/v1/<resource>` list endpoints and incrementally polls
`/v1/events` for change events. See `source_stripe_native/models.py` for the stream
catalog and `source_stripe_native/resources.py` for binding wiring.

## API Rate Limits

- General: **100 req/sec** in live mode, **25 req/sec** in sandbox/test mode
  (per Stripe docs: https://docs.stripe.com/rate-limits).
- Tighter buckets:
  - Files API: 20 read + 20 write req/sec
  - Search API: 20 read req/sec
  - Create Payout API: 15 req/sec, 30 concurrent per business
- 429 behavior: exponential backoff with jitter recommended. SDKs auto-retry 429s
  for lock timeouts.

All buckets are well above the 20 req/hr threshold that triggers the add-stream
skill's budget rule, so `flowctl preview`, `pytest`, and live captures can run
freely without per-call consent (subject to `config.yaml` being clean — Law #4).

## Silent-default-filter audit (as of 2026-06-01)

Some Stripe list endpoints might filter by an `active`/`status` field by default,
which would silently drop archived items and become a data-loss bug. **Verify
empirically before trusting the docs** — `/v1/prices` is documented as
active-only but actually returns both states by default.

| Endpoint | Documented default | Observed behavior | Action |
|---|---|---|---|
| `/v1/prices` | Docs explicit: *"Returns a list of your active prices"* | Bare list returns **both** active and inactive. Docs are misleading. | No partitioning needed. |
| `/v1/plans` | Docs ambiguous on default. | Test account has no inactive plans; not verified. | Verify on a production account with archived plans before assuming. |
| `/v1/products` | Docs ambiguous on default. | Same caveat as Plans. | Same. |
| `/v1/promotion_codes` | Docs ambiguous on default. | Same caveat. | Same. |
| `/v1/coupons` | No `active` parameter at all. `valid` is runtime-computed, not filterable. | N/A. | N/A. |
| `/v1/subscriptions`, `/v1/subscriptions/{id}` (items) | Defaults filtered, but supports `status=all` sentinel. | Already handled in `api.py:233-234`. | None. |

When adding a new stream, run the new endpoint's bare list against an account
with both states present and inspect the response. If the bare list silently
drops a partition that the connector should capture, raise the issue with the
team to decide on a partitioning strategy — there's no precedent in this
connector for needing one.
