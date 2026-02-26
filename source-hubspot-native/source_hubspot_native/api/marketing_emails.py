from datetime import UTC, datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from ..models import (
    MarketingEmail,
    PageResult,
)
from .shared import (
    dt_to_str,
    HUB,
    EPOCH_PLUS_ONE_SECOND,
)


MARKETING_EMAILS_PAGE_SIZE = 300


async def _paginate_through_marketing_emails(
    log: Logger,
    http: HTTPSession,
    params: dict[str, str | int],
) -> AsyncGenerator[MarketingEmail, None]:
    url = f"{HUB}/marketing/v3/emails"

    input: dict[str, str | int] = {}
    input.update(params)

    while True:
        response = PageResult[MarketingEmail].model_validate_json(
            await http.request(log, url, params=input)
        )

        for email in response.results:
            yield email

        if not response.paging:
            break

        input["after"] = response.paging.next.after


async def _fetch_marketing_emails_updated_between(
    log: Logger,
    http: HTTPSession,
    start: datetime,
    end: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:
    if not end:
        end = datetime.now(tz=UTC)

    assert start < end

    params = {
        "limit": MARKETING_EMAILS_PAGE_SIZE,
        "sort": "updatedAt",
        # updatedAfter is an exclusive lower bound filter on the updatedAt field.
        "updatedAfter": dt_to_str(start),
        # updatedBefore is an exclusive upper bound filter on the updatedAt field.
        "updatedBefore": dt_to_str(end),
    }

    # The /marketing/v3/emails API has a bug: when "includeStats=true", HubSpot returns
    # an incomplete subset of emails, omitting some records entirely. When "includeStats=false",
    # all emails are returned but without statistics. To work around this API limitation and
    # ensure we capture all records, we use a two-pass approach: 1) fetch emails with stats
    # enabled and yield all returned records, 2) fetch all emails without stats and yield
    # only those records that were missing from the first pass.
    # Additionally, we have to make separate queries for archived and non-archived records.
    # NOTE: This means yielded documents are *not* ordered by the updatedAt field.

    seen_ids: set[str] = set()
    for archived_query_param in ["false", "true"]:
        for include_stats_query_param in ["true", "false"]:
            params["archived"] = archived_query_param
            params["includeStats"] = include_stats_query_param
            async for email in _paginate_through_marketing_emails(log, http, params):
                if email.id not in seen_ids:
                    seen_ids.add(email.id)
                    yield (email.updatedAt, email.id, email)


async def fetch_marketing_emails_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[MarketingEmail | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    start = EPOCH_PLUS_ONE_SECOND

    # Since we must make multiple separate queries to fetch all marketing emails updated in a
    # certain date window, results are unordered and we cannot emit a checkpoint until all
    # results in the date window from start to end are yielded. For now, fetch_marketing_emails_page
    # is simple and tries to fetch all marketing emails in a single sweep. Typically, most accounts don't
    # have tons of marketing emails, so this backfill doesn't take very long to complete.
    async for _, _, email in _fetch_marketing_emails_updated_between(
        log, http, start=start, end=cutoff
    ):
        yield email


def fetch_recent_marketing_emails(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:

    return _fetch_marketing_emails_updated_between(log, http, since, until)


def fetch_delayed_marketing_emails(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:

    return _fetch_marketing_emails_updated_between(log, http, since, until)
