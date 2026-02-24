from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    ApiKeyInfoResponse,
    AshbyEntity,
    ResponseMeta,
)


API_BASE_URL = "https://api.ashbyhq.com"


async def fetch_api_key_scopes(http: HTTPSession, log: Logger) -> set[str]:
    response = await http.request(log, f"{API_BASE_URL}/apiKey.info", method="POST")
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


async def fetch_entity(
    entity_cls: type[AshbyEntity],
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AshbyEntity | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    sync_token = str(log_cursor[0])

    url = f"{API_BASE_URL}/{entity_cls.path}"
    next_page_cursor: str | None = None
    latest_sync_token: str | None = None
    at_least_one_doc_yielded = False

    while True:
        body: dict[str, str] = {}
        if sync_token:
            body["syncToken"] = sync_token
        if next_page_cursor:
            body["cursor"] = next_page_cursor

        _, response = await http.request_stream(log, url, method="POST", json=body)

        processor = IncrementalJsonProcessor(
            response(), "results.item", entity_cls, remainder_cls=ResponseMeta
        )

        async for item in processor:
            yield item
            at_least_one_doc_yielded = True

        meta = processor.get_remainder()

        if not meta.success:
            if "sync_token_expired" in meta.errors:
                # Tokens expire if they go unused for ~1 month. Since it's a rare
                # occurrence, we'll default to performing a backfill.
                log.warning(
                    f"syncToken expired for {entity_cls.name}, yielding all documents"
                )

                sync_token = ""
                next_page_cursor = None
                continue

            raise RuntimeError(f"Ashby API error for {entity_cls.name}: {meta.errors}")

        if meta.syncToken:
            latest_sync_token = meta.syncToken

        if not meta.moreDataAvailable:
            break

        next_page_cursor = meta.nextCursor

    if at_least_one_doc_yielded and latest_sync_token:
        yield (latest_sync_token,)
