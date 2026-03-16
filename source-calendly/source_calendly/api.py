from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    API_BASE_URL,
    CalendlyChildEntity,
    CalendlyEntity,
    CalendlyResponse,
    EndpointConfig,
    EventInvitee,
    ScheduledEvent,
    UserInfo,
    UserInfoResponse,
)

MAX_PAGE_SIZE = 100


async def is_teams_account(
    org_uri: str,
    http: HTTPSession,
    log: Logger,
) -> bool:
    """Check if the account has team features by probing a team-restricted endpoint."""
    url = API_BASE_URL + "/routing_forms"
    params: dict[str, str | int] = {"organization": org_uri, "count": 1}

    try:
        _ = await http.request(log, url, params=params)
        return True
    except HTTPError as err:
        if err.code == 403:
            log.info("Account does not have team features (routing_forms returned 403)")
            return False
        raise


async def fetch_user_info(http: HTTPSession, log: Logger) -> UserInfo:
    url = API_BASE_URL + "/users/me"

    try:
        response = await http.request(log, url)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    "Invalid credentials. Please confirm the provided access token is correct."
                ]
            ) from err
        raise

    return UserInfoResponse.model_validate_json(response).resource


async def _fetch_entity[T: CalendlyEntity](
    url: str,
    entity_cls: type[T],
    http: HTTPSession,
    log: Logger,
    params: dict[str, str | int],
) -> AsyncGenerator[T, None]:
    params = {**params, "count": MAX_PAGE_SIZE}

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            "collection.item",
            entity_cls,
            remainder_cls=CalendlyResponse,
        )

        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        next_token = remainder.pagination.next_page_token if remainder else None

        if not next_token:
            break

        params = {**params, "page_token": next_token}


async def snapshot_entity(
    entity_cls: type[CalendlyEntity],
    org_uri: str,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[CalendlyEntity, None]:
    url = API_BASE_URL + entity_cls.path
    count = 0

    async for item in _fetch_entity(
        url, entity_cls, http, log, {"organization": org_uri}
    ):
        yield item
        count += 1

    log.info(f"Fetched {count} {entity_cls.name}")


async def snapshot_child_entity(
    entity_cls: type[CalendlyChildEntity],
    org_uri: str,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[CalendlyChildEntity, None]:
    parent_cls = entity_cls.parent_cls
    parent_url = API_BASE_URL + parent_cls.path
    total_count = 0

    async for parent in _fetch_entity(
        parent_url, parent_cls, http, log, {"organization": org_uri}
    ):
        child_url = entity_cls.get_child_url(parent.uri)
        child_params = entity_cls.get_child_params(parent.uri)
        count = 0

        async for child in _fetch_entity(
            child_url, entity_cls, http, log, child_params
        ):
            yield child
            count += 1

        log.info(f"Fetched {count} {entity_cls.name} for {parent.uri}")
        total_count += count

    log.info(f"Fetched {total_count} total {entity_cls.name}")


async def backfill_entity(
    entity_cls: type[CalendlyEntity],
    org_uri: str,
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[CalendlyEntity | PageCursor, None]:
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start_date = datetime.fromisoformat(page) if page is not None else config.start_date

    if start_date >= cutoff:
        return

    url = f"{API_BASE_URL}{entity_cls.path}"
    params: dict[str, str | int] = {"organization": org_uri, "sort": "updated_at:desc"}
    doc_count = 0

    async for item in _fetch_entity(url, entity_cls, http, log, params):
        if item.updated_at >= cutoff:
            continue

        if item.updated_at <= start_date:
            break

        doc_count += 1
        yield item

    log.info(f"Backfilled {doc_count} {entity_cls.name}")


async def fetch_entity(
    entity_cls: type[CalendlyEntity],
    org_uri: str,
    http: HTTPSession,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[CalendlyEntity | LogCursor, None]:
    assert isinstance(cursor, datetime)

    url = API_BASE_URL + entity_cls.path
    params: dict[str, str | int] = {"organization": org_uri, "sort": "updated_at:desc"}
    new_cursor = cursor
    doc_count = 0

    async for item in _fetch_entity(url, entity_cls, http, log, params):
        if item.updated_at <= cursor:
            break

        new_cursor = max(new_cursor, item.updated_at)
        doc_count += 1
        yield item

    log.info(f"Fetched {doc_count} {entity_cls.name} changes")

    if new_cursor > cursor:
        yield new_cursor


def _event_window_params(
    org_uri: str, config: EndpointConfig, now: datetime
) -> dict[str, str | int]:
    start_date = now - timedelta(days=config.scheduled_event_lookback_months * 30)
    end_date = now + timedelta(days=config.scheduled_event_lookahead_months * 30)

    return {
        "organization": org_uri,
        "sort": "start_time:asc",
        "min_start_time": start_date.isoformat(),
        "max_start_time": end_date.isoformat(),
    }


async def fetch_scheduled_events(
    org_uri: str,
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[ScheduledEvent | LogCursor, None]:
    assert isinstance(cursor, datetime)

    now = datetime.now(tz=UTC)
    params = _event_window_params(org_uri, config, now)
    url = API_BASE_URL + ScheduledEvent.path
    doc_count = 0

    async for item in _fetch_entity(url, ScheduledEvent, http, log, params):
        doc_count += 1
        yield item

    log.info(f"Fetched {doc_count} scheduled_events in window")

    yield now


async def fetch_event_invitees(
    org_uri: str,
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[EventInvitee | LogCursor, None]:
    assert isinstance(cursor, datetime)

    now = datetime.now(tz=UTC)
    params = _event_window_params(org_uri, config, now)
    url = API_BASE_URL + ScheduledEvent.path
    total_count = 0

    async for parent in _fetch_entity(url, ScheduledEvent, http, log, params):
        child_url = EventInvitee.get_child_url(parent.uri)
        child_params = EventInvitee.get_child_params(parent.uri)

        async for child in _fetch_entity(
            child_url, EventInvitee, http, log, child_params
        ):
            total_count += 1
            yield child

    log.info(f"Fetched {total_count} event_invitees in window")

    yield now
