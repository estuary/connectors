import base64
import json
import re
from collections.abc import AsyncGenerator, Mapping
from datetime import UTC, datetime
from functools import cache
from logging import Logger
from typing import Any, Literal, cast

from azure.core.async_paging import AsyncPageIterator
from azure.core.pipeline import PipelineResponse
from azure.cosmos.aio import CosmosClient
from estuary_cdk.capture.common import LogCursor, Triggers

from .models import ChangeFeedContinuation, Container, ContainerProperties, Database, Document, PartitionKey


MAX_PAGES_PER_POLL = 100
START_TIME_CURSOR_PREFIX = "start_time:"
SDK_CONTINUATION_PREFIX = "sdk_continuation:"
BEGINNING_CURSOR = "beginning"


@cache
def sdk_reads_point_in_time_as_seconds() -> bool:
    try:
        from azure.cosmos._change_feed.change_feed_start_from import ChangeFeedStartFromInternal

        # An early date keeps even the buggy reader within datetime's range.
        start_time = datetime(1970, 1, 2, 3, 4, 5, 123000, tzinfo=UTC)
        serialized = ChangeFeedStartFromInternal.from_start_time(start_time).to_dict()
        if serialized != {"Type": "PointInTime", "PointInTimeMs": int(start_time.timestamp() * 1000)}:
            raise ValueError("Unexpected start-from serialization")
        if ChangeFeedStartFromInternal.from_json(serialized).to_dict() == serialized:
            return False
        adapted = {**serialized, "PointInTimeMs": serialized["PointInTimeMs"] / 1000}
        if ChangeFeedStartFromInternal.from_json(adapted).to_dict() == serialized:
            return True
        raise ValueError("Unexpected start-from timestamp reader")
    except Exception as err:
        raise RuntimeError(
            "Cannot verify the installed azure-cosmos SDK's change feed timestamp serialization. "
            "Its start-from API has changed; update the connector before capturing."
        ) from err


def continuation_for_sdk(continuation: str) -> str:
    # Some SDKs write PointInTimeMs in milliseconds but read it as seconds.
    # Adapt only the SDK input; checkpoints retain the original token and ranges.
    token = ChangeFeedContinuation.model_validate_json(base64.b64decode(continuation))
    changed = False
    if (
        isinstance(token.startFrom, ChangeFeedContinuation.PointInTime)
        and sdk_reads_point_in_time_as_seconds()
    ):
        token.startFrom.PointInTimeMs /= 1000
        changed = True
    # The decoder also rejects null ETags for unread ranges; empty means no ETag.
    for feed_range in token.continuation.continuation:
        if feed_range.token is None:
            feed_range.token = ""
            changed = True
    return base64.b64encode(token.model_dump_json().encode()).decode() if changed else continuation


PARTITION_KEY_STATE = "partitionKeyState"
# Written in place of a null or undefined partition key value, so key fields
# are never null. The state field tells these apart from real values.
KEY_DEFAULTS: dict[str, Any] = {"string": "", "integer": 0, "boolean": False}
KEY_TYPE_NAMES = {type(default): name for name, default in KEY_DEFAULTS.items()}
PARTITION_PATH_SEGMENT = re.compile(r'''/(?:"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)'|((?!["'])[^/]+))''')


def parse_partition_path(path: str) -> list[str]:
    # Cosmos paths may quote a segment containing a slash; they are not JSON Pointers.
    segments = []
    offset = 0
    while offset < len(path):
        match = PARTITION_PATH_SEGMENT.match(path, offset)
        if match is None:
            raise ValueError(f"Invalid partition key path: {path!r}")
        quoted_double, quoted_single, unquoted = match.groups()
        segments.append(
            unquoted.strip() if unquoted is not None
            else quoted_double if quoted_double is not None else quoted_single
        )
        offset = match.end()
    if not segments:
        raise ValueError("Partition key path must not be empty.")
    return segments


def has_partition_key_state(partition_key: PartitionKey) -> bool:
    # Item IDs always exist, so a container keyed only on /id needs no state.
    return [parse_partition_path(path) for path in partition_key.paths] != [["id"]]


def partition_key_defaults(schema: dict[str, Any], partition_paths: list[list[str]]) -> list[Any]:
    """Default value for each partition key path, by its type in the collection schema.

    The key type is read from the bound collection rather than sampled again, so
    a default always matches the type that materializations were set up with.
    """
    defaults = []
    for segments in partition_paths:
        if segments == ["id"]:
            defaults.append("")
            continue
        node: Any = schema
        for field in segments:
            node = node.get("properties", {}).get(field) if isinstance(node, dict) else None
        types = node.get("type") if isinstance(node, dict) else None
        types = [types] if isinstance(types, str) else [
            type_ for type_ in types or [] if type_ != "null"
        ]
        if len(types) != 1 or types[0] not in KEY_DEFAULTS:
            raise RuntimeError(
                f"Cannot determine the type of partition key '/{'/'.join(segments)}' from the collection schema: "
                f"expected exactly one of {', '.join(KEY_DEFAULTS)}, found {types or 'none'}. "
                "Re-discover the binding to restore its schema."
            )
        defaults.append(KEY_DEFAULTS[types[0]])
    return defaults


def matches_key_type(value: Any, default: Any) -> bool:
    # bool is a subclass of int, so check it first. Discovery types integral
    # floats as integers, so accept them here too.
    if isinstance(default, bool) or isinstance(value, bool):
        return isinstance(default, bool) and isinstance(value, bool)
    if isinstance(default, int):
        return isinstance(value, int) or (isinstance(value, float) and value.is_integer())
    return isinstance(value, str)


def fill_partition_keys(
    document: dict[str, Any], partition_paths: list[list[str]], defaults: list[Any],
) -> str:
    """Write a non-null value at each partition key path and return the key state.

    Cosmos DB stores items without a partition key value (including legacy
    items in systemKey containers) under the undefined partition. Capture them
    with the type's default, and record per path whether the item had a value,
    an explicit null, or no value, so these items keep distinct keys.
    """
    states = []
    for segments, default in zip(partition_paths, defaults):
        node = document
        for field in segments[:-1]:
            if not isinstance(node.get(field), dict):
                # A missing, null or scalar parent can't hold the key, and the
                # schema requires an object here, so replace it.
                node[field] = {}
            node = node[field]
        if segments[-1] not in node:
            states.append("undefined")
        elif node[segments[-1]] is None:
            states.append("null")
        else:
            value = node[segments[-1]]
            if not matches_key_type(value, default):
                # Another type would fail the collection schema, so name the item.
                # Re-discovery can only pick a string, integer or boolean key, and
                # samples an integral float as an integer.
                if isinstance(value, (str, int)) or (isinstance(value, float) and value.is_integer()):
                    value_key_type = KEY_TYPE_NAMES[int if isinstance(value, float) else type(value)]
                    remedy = (
                        f"If every item's value at this path is of type {value_key_type}, re-discover and backfill "
                        "the binding to change the key type. Otherwise, disable the binding."
                    )
                else:
                    remedy = "This value cannot be used as a collection key, so disable the binding."
                raise RuntimeError(
                    f"Item '{document.get('id')}' has a {type(value).__name__} value at partition key "
                    f"'/{'/'.join(segments)}', which does not match the collection key type "
                    f"({KEY_TYPE_NAMES[type(default)]}). {remedy}"
                )
            if (
                isinstance(default, int) and not isinstance(default, bool)
                and isinstance(value, float) and value.is_integer()
                and -(2**63) <= value < 2**64
            ):
                # Flow packs floats and integers differently, even for equal values.
                node[segments[-1]] = int(value)
            states.append("value")
            continue
        node[segments[-1]] = default
    return ",".join(states)


async def list_databases(client: CosmosClient) -> list[str]:
    return [Database.model_validate(database).id async for database in client.list_databases()]


async def list_containers(client: CosmosClient, database: str) -> list[Container]:
    return [
        Container.model_validate(container)
        async for container in client.get_database_client(database).list_containers()
    ]


async def sample_partition_key(
    client: CosmosClient, database: str, container: str, partition_path: str,
    priority: Literal["High", "Low"] | None = None,
) -> Any:
    source = client.get_database_client(database).get_container_client(container)
    path = "c" + "".join(
        f"[{json.dumps(segment)}]" for segment in parse_partition_path(partition_path)
    )
    return await anext(aiter(source.query_items(
        # A null says nothing about the key's type, so prefer a non-null value.
        query=f"SELECT TOP 1 VALUE {path} FROM c WHERE IS_DEFINED({path}) AND NOT IS_NULL({path})",
        max_item_count=1,
        priority=priority,
        # Containers with indexing disabled reject filtered queries without a scan.
        enable_scan_in_query=True,
    )), None)


async def fetch_container(
    client: CosmosClient,
    database: str,
    container: str,
    log: Logger,
    cursor: LogCursor,
    *,
    properties_cache: dict[tuple[str, str], ContainerProperties],
    schema: dict[str, Any],
    start_time: Literal["Beginning", "Now"] | datetime = "Beginning",
    priority: Literal["High", "Low"] | None = None,
    max_item_count: int | None = None,
) -> AsyncGenerator[Document | LogCursor | Triggers, None]:
    """Start at the configured position, then resume from the last captured page.

    The SDK continuation tracks each feed range, including splits.
    A page's continuation is checkpointed only after all its documents.
    After initialization, a caught-up feed yields nothing, so the CDK waits.
    Busy feeds return at a page boundary so the CDK can check for shutdown.
    """
    assert isinstance(cursor, tuple) and isinstance(cursor[0], str)

    continuation = cursor[0]
    saved_continuation = continuation.removeprefix(SDK_CONTINUATION_PREFIX)
    if continuation == BEGINNING_CURSOR:
        start_time = "Beginning"
        continuation = ""
    if not continuation and start_time == "Now":
        start_time = datetime.now(UTC).replace(microsecond=0)
    if not continuation and isinstance(start_time, datetime):
        # Persist the start before the first request, even if it fails.
        continuation = START_TIME_CURSOR_PREFIX + start_time.isoformat()
        yield (continuation,)
    if continuation.startswith(START_TIME_CURSOR_PREFIX):
        start_time = datetime.fromisoformat(continuation[len(START_TIME_CURSOR_PREFIX):])
        continuation = ""
    if continuation:
        if not continuation.startswith(SDK_CONTINUATION_PREFIX):
            raise ValueError("Invalid change feed cursor: expected a prefixed SDK continuation.")
        continuation = continuation_for_sdk(continuation[len(SDK_CONTINUATION_PREFIX):])

    source = client.get_database_client(database).get_container_client(container)
    if (database, container) not in properties_cache:
        properties_cache[(database, container)] = ContainerProperties.model_validate(await source.read())
    properties = properties_cache[(database, container)]
    partition_key = properties.partitionKey
    partition_paths = [parse_partition_path(path) for path in partition_key.paths]
    key_state = has_partition_key_state(partition_key)
    defaults = partition_key_defaults(schema, partition_paths) if key_state else []
    response_headers: Mapping[str, str] = {}

    def response_hook(headers: Mapping[str, str], _: Any) -> None:
        nonlocal response_headers
        # Keep the mapping: the SDK replaces its ETag with the composite token
        # after calling the hook, including on empty responses.
        response_headers = headers

    def raw_response_hook(response: PipelineResponse) -> None:
        # A retry can refresh metadata while retaining the old feed cursor.
        # Check its public request headers before emitting a page or checkpoint.
        rid = response.http_request.headers.get("x-ms-cosmos-intended-collection-rid")
        if rid is not None and rid != properties.rid:
            raise ValueError("Invalid continuation: [Mismatch collection rid]")

    pages = cast(
        AsyncPageIterator[dict[str, Any]],
        source.query_items_change_feed(
            mode="LatestVersion",
            start_time=start_time,
            continuation=continuation or None,
            priority=priority,
            max_item_count=max_item_count,
            response_hook=response_hook,
            raw_response_hook=raw_response_hook,
        ).by_page(),
    )

    page_count = 0
    try:
        async for page in pages:
            async for item in page:
                document = {
                    name: value for name, value in item.items()
                    if name not in ("_self", "_attachments", "_meta")
                }
                if key_state:
                    state = fill_partition_keys(document, partition_paths, defaults)
                    yield Document(_meta=Document.Meta(op="u", partitionKeyState=state), **document)
                else:
                    yield Document(_meta=Document.Meta(op="u"), **document)

            continuation = pages.continuation_token
            if not continuation:
                raise RuntimeError("Cosmos DB returned a change feed page without a continuation.")
            yield (SDK_CONTINUATION_PREFIX + continuation,)
            saved_continuation = continuation
            page_count += 1
            if page_count >= MAX_PAGES_PER_POLL:
                return
        # The page iterator's continuation may be stale after a 304.
        continuation = response_headers.get("etag")
        if not continuation:
            raise RuntimeError("Cosmos DB returned an empty change feed without a continuation.")
        if continuation != saved_continuation:
            yield (SDK_CONTINUATION_PREFIX + continuation,)
    except ValueError as err:
        if "Mismatch collection rid" not in str(err):
            raise
        del properties_cache[(database, container)]
        log.warning(
            "Cosmos DB container was recreated; restarting from Beginning."
            if start_time == "Now" else
            "Cosmos DB container was recreated; requesting a backfill.",
            {"container": container},
        )
        # The SDK caches partition ranges by container name, not rid, so this
        # client would otherwise read the new container with the old ranges.
        async for _ in source.read_feed_ranges(force_refresh=True):
            pass
        yield (BEGINNING_CURSOR,) if start_time == "Now" else Triggers.BACKFILL
