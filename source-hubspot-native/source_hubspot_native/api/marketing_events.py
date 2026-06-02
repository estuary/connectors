from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, JsonValue

from ..models import MarketingEvent, MarketingEventParticipant, PageResult
from .shared import HUB

PAGE_SIZE = 100


class _ResponseRemainder(BaseModel, extra="allow"):
    paging: PageResult.Paging | None = None


async def fetch_marketing_events(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[MarketingEvent, None]:
    url = f"{HUB}/marketing/v3/marketing-events"
    after: str | None = None

    input: dict[str, JsonValue] = {"limit": PAGE_SIZE}

    while True:
        if after:
            input["after"] = after

        _, body = await http.request_stream(log, url, method="GET", params=input)
        processor = IncrementalJsonProcessor(
            body(),
            "results.item",
            MarketingEvent,
            remainder_cls=_ResponseRemainder,
        )

        async for event in processor:
            yield event

        paging = processor.get_remainder().paging
        if not paging:
            return

        after = paging.next.after


async def fetch_marketing_event_participants(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[MarketingEventParticipant, None]:
    event_ids = [event.objectId async for event in fetch_marketing_events(http, log)]

    for event_id in event_ids:
        url = (
            f"{HUB}/marketing/v3/marketing-events/participations/"
            f"{event_id}/breakdown"
        )
        after: str | None = None
        input: dict[str, JsonValue] = {"limit": PAGE_SIZE}

        while True:
            if after:
                input["after"] = after

            _, body = await http.request_stream(log, url, method="GET", params=input)
            processor = IncrementalJsonProcessor(
                body(),
                "results.item",
                MarketingEventParticipant,
                remainder_cls=_ResponseRemainder,
            )

            async for participant in processor:
                yield participant

            paging = processor.get_remainder().paging
            if not paging:
                break

            after = paging.next.after
