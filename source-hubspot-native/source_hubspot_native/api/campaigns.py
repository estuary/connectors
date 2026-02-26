from datetime import UTC, datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from .properties import fetch_properties

from ..models import (
    Campaign,
    Names,
    PageResult,
)
from .shared import (
    EPOCH_PLUS_ONE_SECOND,
    HUB,
    DELAYED_LAG,
)

# Allowed readable properties for campaigns. HubSpot's /crm/v3/properties/campaign
# endpoint returns ALL properties, but the /marketing/v3/campaigns endpoint only
# allows a subset to be read. Requesting forbidden properties returns a 400 error.
CAMPAIGN_READABLE_PROPERTIES = {
    "hs_start_date",
    "hs_end_date",
    "hs_color_hex",
    "hs_notes",
    "hs_audience",
    "hs_goal",
    "hs_owner",
    "hs_currency_code",
    "hs_created_by_user_id",
    "hs_campaign_status",
    "hs_object_id",
    "hs_name",
    "hs_utm",
    "hs_budget_items_sum_amount",
    "hs_spend_items_sum_amount",
}

def _is_readable_campaign_property(prop_name: str) -> bool:
    # Custom properties don't start with hs_ or hubspot_
    return prop_name in CAMPAIGN_READABLE_PROPERTIES or (
        not prop_name.startswith("hs_") and not prop_name.startswith("hubspot_")
    )


async def check_campaigns_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Campaign, None]:
    url = f"{HUB}/marketing/v3/campaigns"

    response = PageResult[Campaign].model_validate_json(
        await http.request(log, url, params={"limit": 1})
    )

    for campaign in response.results:
        yield campaign


async def _request_campaigns_in_time_range(
    http: HTTPSession, log: Logger, start: datetime, end: datetime | None = None
) -> AsyncGenerator[Campaign, None]:
    properties = await fetch_properties(log, http, "campaign")
    property_names = ",".join(
        p.name for p in properties.results if _is_readable_campaign_property(p.name)
    )

    url = f"{HUB}/marketing/v3/campaigns"
    params: dict[str, str | int] = {
        "limit": 100,
        "sort": "-updatedAt",  # Descending order
        "properties": property_names,
    }

    while True:
        response = PageResult[Campaign].model_validate_json(
            await http.request(log, url, params=params)
        )

        for campaign in response.results:
            if end and campaign.updatedAt >= end:
                continue
            if campaign.updatedAt < start:
                # Descending order - we're past the time range
                return

            yield campaign

        if not response.paging:
            return

        params["after"] = response.paging.next.after


async def fetch_campaigns_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Campaign | PageCursor, None]:
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start_date = (
        datetime.fromisoformat(page) if page is not None else EPOCH_PLUS_ONE_SECOND
    )

    async for campaign in _request_campaigns_in_time_range(
        http, log, start_date, cutoff
    ):
        yield campaign


async def fetch_campaigns(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Campaign | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # Look back by DELAYED_LAG to catch any campaigns that may have been
    # updated but not yet visible in the API due to eventual consistency.
    start_date = log_cursor - DELAYED_LAG
    now = datetime.now(tz=UTC)

    async for item in _request_campaigns_in_time_range(http, log, start_date):
        if cache.should_yield(Names.campaigns, item.id, item.updatedAt):
            yield item

    yield now
