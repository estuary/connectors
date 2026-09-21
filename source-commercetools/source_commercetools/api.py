from base64 import b64encode
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    CommercetoolsResource,
    PagedQueryResponse,
    Region,
    TokenIntrospection,
)

# commercetools' documented maximum for a Query endpoint.
PAGE_LIMIT = 500

# commercetools timestamps carry milliseconds, so that is the finest distinction a
# cursor can make.
SMALLEST_DATETIME_GRAIN = timedelta(milliseconds=1)

# How far behind the present `fetch_resources` stays to hopefully
# combat distributed clock issues & potential eventually consistent API behavior.
SETTLE_DELAY = timedelta(minutes=5)

# Documents to emit before looking for a checkpoint. One is only taken at a
# millisecond boundary, so this is a floor rather than a batch size.
CHECKPOINT_INTERVAL = 1000


def base_url(region: Region, project_key: str) -> str:
    return f"https://api.{region}.commercetools.com/{project_key}"


async def introspect_token(
    http: HTTPSession,
    region: Region,
    client_id: str,
    client_secret: str,
    token: str,
    log: Logger,
) -> TokenIntrospection:
    """Introspect an access token, reporting whether it is active and its scopes.

    No scope can block this. A client may always introspect its own tokens. It
    authenticates as the client with HTTP Basic rather than with the token it is
    asking about, so it bypasses the session's `TokenSource`.
    """
    basic = b64encode(f"{client_id}:{client_secret}".encode()).decode()

    body = await http.request(
        log,
        f"https://auth.{region}.commercetools.com/oauth/introspect",
        method="POST",
        form={"token": token},
        with_token=False,
        headers={"Authorization": f"Basic {basic}"},
    )

    return TokenIntrospection.model_validate_json(body)


def granted_permissions(scope: str, project_key: str) -> set[str]:
    """The permission names a token holds within `project_key`.

    A scope is `{permission}:{projectKey}`, with a third segment for Store-scoped
    clients (`view_orders:my-project:my-store`). Scopes for other Projects are
    dropped.
    """
    permissions = set()

    for entry in scope.split():
        parts = entry.split(":")
        if len(parts) >= 2 and parts[1] == project_key:
            permissions.add(parts[0])

    return permissions


def is_accessible(permissions: set[str], path: str) -> bool:
    """Whether `permissions` allow reading the resource at `path`.

    `manage_` subsumes its `view_` counterpart, and `manage_project` subsumes both.
    """
    return bool(
        permissions & {f"view_{path}", f"manage_{path}", "manage_project"}
    )


def floor_tick(dt: datetime) -> datetime:
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _dt_to_str(dt: datetime) -> str:
    """Render `dt` as a commercetools DateTime predicate literal."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _after(cursor: datetime, last_id: str | None) -> str:
    """Predicate matching what sorts after `(cursor, last_id)`.

    This is the keyset resume for `sort=lastModifiedAt asc&sort=id asc`: it walks a
    result set of any size without `offset`, which commercetools caps at 10,000.
    Without a `last_id` it is just the open lower bound that starts a walk.
    """
    if last_id is None:
        return f'lastModifiedAt > "{_dt_to_str(cursor)}"'

    return (
        f'(lastModifiedAt > "{_dt_to_str(cursor)}" or '
        f'(lastModifiedAt = "{_dt_to_str(cursor)}" and id > "{last_id}"))'
    )


async def _query(
    http: HTTPSession,
    log: Logger,
    url: str,
    model: type[CommercetoolsResource],
    where: list[str],
    sort: list[str],
) -> AsyncGenerator[CommercetoolsResource, None]:
    """Yield one page of a Query endpoint's results, in the requested sort order."""
    request_params: dict[str, str | int | list[str]] = {
        "where": " and ".join(where),
        "sort": sort,
        "limit": PAGE_LIMIT,
        # `total` costs a second count query and this connector never reads it.
        "withTotal": "false",
    }

    _, body = await http.request_stream(log, url, params=request_params)
    processor = IncrementalJsonProcessor(
        body(),
        "results.item",
        model,
        PagedQueryResponse,
    )

    async for doc in processor:
        yield doc

    # Validates the envelope around `results`. Without it, a response that is not a
    # PagedQueryResponse streams zero items, which callers read as a drained window.
    processor.get_remainder()


async def fetch_resources(
    http: HTTPSession,
    region: Region,
    project_key: str,
    model: type[CommercetoolsResource],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CommercetoolsResource | LogCursor, None]:
    """Emit resources modified between `log_cursor` and one `SETTLE_DELAY` ago."""
    assert isinstance(log_cursor, datetime)

    horizon = (
        floor_tick(datetime.now(tz=UTC)) - SETTLE_DELAY - SMALLEST_DATETIME_GRAIN
    )

    if horizon <= log_cursor:
        return

    url = f"{base_url(region, project_key)}/{model.PATH}"
    upper = f'lastModifiedAt <= "{_dt_to_str(horizon)}"'

    # The walk's position: the cursor and id of the last document emitted.
    position, last_id = log_cursor, None
    emitted_since_checkpoint = 0

    while True:
        page_size = 0

        async for doc in _query(
            http,
            log,
            url,
            model,
            where=[_after(position, last_id), upper],
            sort=["lastModifiedAt asc", "id asc"],
        ):
            page_size += 1
            doc_cursor = doc.get_cursor()

            if doc_cursor < position:
                msg = "Received documents out of order from the commercetools API."
                log.error(
                    msg,
                    {
                        "stream": model.PATH,
                        "id": doc.id,
                        "last_modified_at": doc_cursor,
                        "previous_last_modified_at": position,
                    },
                )
                raise RuntimeError(msg)

            # A strictly greater timestamp proves every document at `position` has
            # been emitted, which is what makes `position` safe to checkpoint. The
            # cursor cannot checkpoint mid-millisecond: it carries no id.
            if (
                doc_cursor > position
                and emitted_since_checkpoint >= CHECKPOINT_INTERVAL
            ):
                yield position
                emitted_since_checkpoint = 0

            yield doc
            position, last_id = doc_cursor, doc.id
            emitted_since_checkpoint += 1

        if page_size < PAGE_LIMIT:
            break

    yield horizon


async def backfill_resources(
    http: HTTPSession,
    region: Region,
    project_key: str,
    start_date: datetime,
    model: type[CommercetoolsResource],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[CommercetoolsResource | PageCursor, None]:
    """Emit resources modified between `start_date` and the incremental cutoff."""
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    url = f"{base_url(region, project_key)}/{model.PATH}"
    where = [
        f'lastModifiedAt >= "{_dt_to_str(start_date)}"',
        f'lastModifiedAt <= "{_dt_to_str(cutoff - SMALLEST_DATETIME_GRAIN)}"',
    ]
    if page is not None:
        where.append(f'id > "{page}"')

    page_size = 0
    last_id = None

    async for doc in _query(
        http, log, url, model, where=where, sort=["id asc"]
    ):
        page_size += 1
        last_id = doc.id
        yield doc

    # Only a full page may have more behind it. Returning without a cursor is how the
    # CDK is told the backfill is complete.
    if page_size == PAGE_LIMIT and last_id is not None:
        yield last_id
