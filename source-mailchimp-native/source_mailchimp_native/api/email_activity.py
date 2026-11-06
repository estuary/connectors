import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    EmailActivityBackfillCursor,
    EmailActivityEvent,
    EmailCampaignStub,
    MemberEmailActivity,
)
from .shared import MAX_PAGE_SIZE, fetch_collection_page, snapshot_collection

# Lower-bound re-read overlap: every incremental poll re-reads this much behind
# its cursor. Bruno's `Email Activity / bounce shape` bounds visibility lag at
# ≤240s for a hard bounce; the true bound is UNVERIFIED.
LOOKBACK = timedelta(minutes=10)

# How long after send a campaign keeps being re-checked for bounces even with
# no fresh opens/clicks. Bounces move no `/reports` recency field, so this
# retention is the sole mechanism that observes them. Sized to when common MTAs
# give up retrying a soft bounce (RFC 5321 §4.5.4.1 recommends ≥5 days). Hard
# bounces arrive within seconds and could take a much shorter window, but they
# deliberately share this one: a soft bounce may convert to hard days after
# send when retries exhaust, and whether Mailchimp then reclassifies them as
# hard bounces is unverified — a full-length window on either count keeps the
# campaign open for that late event.
BOUNCE_SETTLE_WINDOW = timedelta(days=5)

_EMAIL_CAMPAIGN_REPORT_FIELDS = (
    "reports.id,reports.send_time,reports.opens.last_open,"
    "reports.clicks.last_click,"
    "reports.bounces.hard_bounces,reports.bounces.soft_bounces"
)


async def _fetch_email_campaign_stubs(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> list[EmailCampaignStub]:
    """Drain `/reports` into recency-gate stubs before descending into any
    campaign. `/reports` returns only campaigns that have a report, so unsent
    drafts never appear."""
    return [
        stub
        async for stub in snapshot_collection(
            http,
            base_url,
            "reports",
            "reports",
            EmailCampaignStub,
            log,
            params={"fields": _EMAIL_CAMPAIGN_REPORT_FIELDS},
        )
    ]


async def fetch_email_activity(
    http: HTTPSession,
    base_url: str,
    start_date: datetime,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[EmailActivityEvent | LogCursor, None]:
    """Incrementally fetch email activity across all reported campaigns.

    Each poll drains `/reports` stubs and descends only into campaigns the
    recency gate selects: fresh opens/clicks, or a campaign that still has
    bounces settling.

         cursor − LOOKBACK    cursor   horizon = last elapsed second
    ───────────────┼────┼────────┼───────────┼────▶ time (1s ticks)
                   │    │        │           │
    SINCE_PARAM ───(════╪════════╪═══════════╪════▶
    emitted ───────┼────[════════╪═══════════]
                   │    │        │           └─ the present second is still
                   │    │        │              in progress; its docs wait for
                   │    │        │              the next poll's window
                   │    │        └─ newest emitted event
                   │    │           timestamp
                   │    └─ first emitted second
                   └─ exclusive; the overlap absorbs
                      ingest lag, and the dedup cache
                      suppresses re-emission
    """
    assert isinstance(log_cursor, datetime)

    # The last fully-elapsed second; events stamped in the still-in-progress
    # second wait for the next poll's window.
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return

    lookback_floor = log_cursor - LOOKBACK
    since = max(start_date, lookback_floor)
    settle_floor = horizon - BOUNCE_SETTLE_WINDOW

    emitted = 0
    max_emitted_ts = log_cursor

    email_campaigns = await _fetch_email_campaign_stubs(http, base_url, log)
    selected = [
        campaign
        for campaign in email_campaigns
        if campaign.has_activity_after(lookback_floor)
        or campaign.has_bounces_settling(settle_floor)
    ]

    for campaign in selected:
        members = snapshot_collection(
            http,
            base_url,
            EmailActivityEvent.PATH_TEMPLATE.format(campaign_id=campaign.id),
            EmailActivityEvent.ITEMS_KEY,
            MemberEmailActivity,
            log,
            params={EmailActivityEvent.SINCE_PARAM: since.isoformat()},
        )
        async for member in members:
            for event in member.activity:
                if event.timestamp > horizon:
                    continue

                if cache.should_yield(
                    EmailActivityEvent.NAME,
                    member.event_cache_key(event),
                    event.timestamp,
                ):
                    emitted += 1
                    max_emitted_ts = max(max_emitted_ts, event.timestamp)
                    yield member.flatten(event)

    evicted = cache.cleanup(EmailActivityEvent.NAME, since)
    log.info(
        "email_activity sweep complete",
        {
            "gated_campaigns": len(selected),
            "reported_campaigns": len(email_campaigns),
            "emitted": emitted,
            "horizon": horizon,
            "checkpoint": max_emitted_ts,
            "cache_evictions": evicted,
        },
    )

    if emitted:
        yield max_emitted_ts


# WARN: This nested iteration solution might choke on campaigns with hundreds
# of thousands of emails. If that's the case, we should consider switching to
# batch operations
# (https://mailchimp.com/developer/marketing/api/batch-operations/).
async def backfill_email_activity(
    http: HTTPSession,
    base_url: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[EmailActivityEvent | PageCursor, None]:
    """Backfill email activity over the frozen `(start_date, cutoff − 1s]`
    window. The first invocation drains `/reports` once, yields the selected
    campaign IDs as the cursor's full state, and returns.

    Checkpoints sit at campaign boundaries only: `emails[]` row order stability
    remains unverified.

    Campaign selection is the incremental recency gate thresholded at
    `start_date`: a campaign *sent* before `start_date` can still have
    in-window opens/clicks, and only its `/reports` recency fields reveal that.

          start_date               cutoff − 1s   cutoff
    ──────────┼─────────────────────────┼───────┼──▶ time (1s ticks)
              │                         │       │
    since ────(═════════════════════════╪═══════╪══▶
    emitted ──┼─[═══════════════════════]       │
              │                         │       └─ first incremental poll
              │                         │          emits from here
              │                         └─ last backfilled second
              └─ minor detail: docs at exactly start_date are skipped
    """
    assert page is None or isinstance(page, dict)
    assert isinstance(cutoff, datetime)

    horizon = cutoff - timedelta(seconds=1)

    if page is None:
        email_campaigns = await _fetch_email_campaign_stubs(http, base_url, log)
        cursor = EmailActivityBackfillCursor.from_campaign_ids(
            [
                stub.id
                for stub in email_campaigns
                if stub.has_activity_after(start_date) or stub.send_time >= start_date
            ]
        )
        log.info(
            "email_activity backfill campaign set derived",
            {
                "gated_campaigns": len(cursor.remaining),
                "reported_campaigns": len(email_campaigns),
            },
        )
        if not cursor.remaining:
            return
        # Yield immediately so we don't have to re-fetch parent ids if the
        # first campaign fails
        yield cursor.create_initial_cursor()
        return

    cursor = EmailActivityBackfillCursor.from_cursor_dict(page)
    if not cursor.remaining:
        return

    campaign_id = next(iter(cursor.remaining))

    started = time.monotonic()
    offset = 0
    campaign_pages = 0
    emitted = 0

    while True:
        members = fetch_collection_page(
            http,
            base_url,
            EmailActivityEvent.PATH_TEMPLATE.format(campaign_id=campaign_id),
            EmailActivityEvent.ITEMS_KEY,
            MemberEmailActivity,
            {
                "count": MAX_PAGE_SIZE,
                "offset": offset,
                EmailActivityEvent.SINCE_PARAM: start_date.isoformat(),
            },
            log,
        )

        page_member_count = 0
        async for member in members:
            page_member_count += 1

            for event in member.activity:
                if event.timestamp <= horizon:
                    yield member.flatten(event)
                    emitted += 1

        campaign_pages += 1
        if page_member_count < MAX_PAGE_SIZE:
            break

        offset += MAX_PAGE_SIZE

    log.info(
        "email_activity backfill campaign drained",
        {
            "campaign_id": campaign_id,
            "seconds": round(time.monotonic() - started, 3),
            "pages": campaign_pages,
            "emitted": emitted,
            "campaigns_left": len(cursor.remaining) - 1,
        },
    )

    if len(cursor.remaining) > 1:
        yield cursor.create_completion_patch(campaign_id)
        return

    log.info("email_activity backfill completed")
