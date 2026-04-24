from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import JsonValue

from .models import (
    ApiKeyInfoResponse,
    AshbyEntity,
    ChildEntityMixin,
    ResponseMeta,
)

API_BASE_URL = "https://api.ashbyhq.com"


async def fetch_api_key_scopes(http: HTTPSession, log: Logger) -> set[str]:
    # Ashby requires Content-Type: application/json on all POST requests.
    # Passing json={} ensures aiohttp sets the header automatically.
    response = await http.request(
        log, f"{API_BASE_URL}/apiKey.info", method="POST", json={}
    )
    info = ApiKeyInfoResponse.model_validate_json(response)

    if info.errorInfo is not None:
        raise ValidationError(
            [
                f"""Invalid API key. Please confirm the provided API key is correct.

                [{info.errorInfo.code}] {info.errorInfo.message}"""
            ]
        )

    assert (
        info.results is not None
    ), "Responses carry either an `errorInfo` or `results` object"

    return set(info.results.scopes)


async def _stream_pages(
    entity_cls: type[AshbyEntity],
    base_body: dict[str, JsonValue],
    http: HTTPSession,
    log: Logger,
    sync_token: str | None = None,
) -> AsyncGenerator[AshbyEntity | ResponseMeta, None]:
    url = f"{API_BASE_URL}/{entity_cls.path}"
    next_page_cursor: str | None = None

    while True:
        body: dict[str, JsonValue] = {**base_body}
        if sync_token and next_page_cursor is None:
            body["syncToken"] = sync_token
        if next_page_cursor:
            body["cursor"] = next_page_cursor

        _, response = await http.request_stream(log, url, method="POST", json=body)
        processor = IncrementalJsonProcessor(
            response(), "results.item", entity_cls, remainder_cls=ResponseMeta
        )

        async for item in processor:
            yield item

        meta = processor.get_remainder()

        # Tokens expire after ~1 month of disuse. Recovery is a fresh listing —
        # restrict the retry to the first response so a mid-pagination error
        # never causes us to re-yield earlier pages.
        if "sync_token_expired" in meta.errors and next_page_cursor is None:
            log.warning(
                f"sync token expired for {entity_cls.name}, yielding all documents"
            )
            sync_token = None
            continue

        if not meta.success:
            raise RuntimeError(
                (
                    f"Ashby API error for {entity_cls.name}: {meta.errors} "
                    f"(request body: {body})"
                )
            )

        yield meta

        if not meta.moreDataAvailable:
            return

        next_page_cursor = meta.nextCursor


async def snapshot_entity(
    entity_cls: type[AshbyEntity],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[AshbyEntity, None]:
    async for item in _stream_pages(
        entity_cls, entity_cls.base_request_body, http, log
    ):
        if isinstance(item, ResponseMeta):
            continue
        yield item


async def snapshot_child_entity(
    entity_cls: type[AshbyEntity],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[AshbyEntity, None]:
    assert issubclass(entity_cls, ChildEntityMixin)

    parent_ids = [
        parent.id
        async for parent in snapshot_entity(entity_cls.parent_entity, http, log)
    ]

    for parent_id in parent_ids:
        body: dict[str, JsonValue] = {
            **entity_cls.base_request_body,
            entity_cls.parent_id_field: parent_id,
        }

        async for item in _stream_pages(entity_cls, body, http, log):
            if isinstance(item, ResponseMeta):
                continue
            yield item


async def fetch_entity(
    entity_cls: type[AshbyEntity],
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AshbyEntity | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    sync_token = str(log_cursor[0]) or None

    new_sync_token: str | None = None
    at_least_one_doc_yielded = False

    async for item in _stream_pages(
        entity_cls, entity_cls.base_request_body, http, log, sync_token=sync_token
    ):
        if isinstance(item, ResponseMeta):
            if item.syncToken:
                new_sync_token = item.syncToken
            continue
        yield item
        at_least_one_doc_yielded = True

    if at_least_one_doc_yielded and new_sync_token is None:
        raise RuntimeError(
            (
                f"Ashby API did not return a syncToken for {entity_cls.name} "
                f"after yielding documents; cannot checkpoint."
            )
        )

    if at_least_one_doc_yielded and new_sync_token:
        yield (new_sync_token,)


async def fetch_incremental_child_entity(
    entity_cls: type[AshbyEntity],
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AshbyEntity | LogCursor, None]:
    assert issubclass(entity_cls, ChildEntityMixin)
    assert isinstance(log_cursor, tuple)

    parent_cls = entity_cls.parent_entity
    parent_token: str | None = str(log_cursor[0]) or None

    # Drain updated parents into a list before issuing per-parent child requests —
    # holding the parent stream open while fetching children risks timing the
    # parent connection out.
    updated_parent_ids: list[str] = []
    new_parent_token: str | None = None

    async for item in _stream_pages(
        parent_cls, parent_cls.base_request_body, http, log, sync_token=parent_token
    ):
        if isinstance(item, ResponseMeta):
            if item.syncToken:
                new_parent_token = item.syncToken
            break
        updated_parent_ids.append(item.id)

    yielded_anything = False

    for parent_id in updated_parent_ids:
        body: dict[str, JsonValue] = {
            **entity_cls.base_request_body,
            entity_cls.parent_id_field: parent_id,
        }

        async for item in _stream_pages(entity_cls, body, http, log):
            if isinstance(item, ResponseMeta):
                continue
            yield item
            yielded_anything = True

    if yielded_anything and new_parent_token is None:
        raise RuntimeError(
            (
                f"Ashby API did not return a syncToken for {parent_cls.name} "
                f"after yielding {entity_cls.name} documents; cannot checkpoint."
            )
        )

    if updated_parent_ids and new_parent_token:
        assert new_parent_token
        yield (new_parent_token,)
