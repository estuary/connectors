import asyncio
import functools
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import CaptureBinding, ResourceConfig, ResourceState
from estuary_cdk.flow import OAuth2TokenFlowSpec, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from estuary_cdk.incremental_csv_processor import BaseCSVRow

from . import api
from .export_manager import ExportManager
from .models import EndpointConfig, ZuoraDocument


_DISCOVER_CONCURRENCY = 10


def _setup_http_auth(http: HTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(
        oauth_spec=OAuth2TokenFlowSpec(
            accessTokenUrlTemplate=f"{config.base_url}/oauth/token",
            accessTokenResponseMap={"access_token": "/access_token"},
        ),
        credentials=config.credentials,
    )


def _open_incremental(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    start_date: datetime,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings,
) -> None:
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_changes=functools.partial(
            api.fetch_changes,
            object_name,
            fields,
            manager,
        ),
        fetch_page=functools.partial(
            api.fetch_page,
            object_name,
            fields,
            manager,
            start_date,
        ),
    )


def _open_snapshot(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings,
) -> None:
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_snapshot=functools.partial(
            api.fetch_snapshot,
            object_name,
            fields,
            manager,
        ),
    )


@dataclass(frozen=True)
class DescribedObject:
    """A queryable Zuora object and its exportable field names."""
    name: str
    fields: list[str]


async def _describe_one(
    object_name: str,
    base_url: str,
    http: HTTPMixin,
    log: Logger,
    sem: asyncio.Semaphore,
) -> DescribedObject | None:
    async with sem:
        try:
            fields = await api.describe_object(base_url, http, log, object_name)
        except HTTPError as err:
            log.warning("Skipping object: describe failed", {"object": object_name, "http_code": err.code})
            return None
        except Exception as err:
            log.warning("Skipping object: describe failed", {"object": object_name, "error": str(err)})
            return None

    if not fields:
        log.warning("Skipping object: no selectable fields", {"object": object_name})
        return None
    return DescribedObject(name=object_name, fields=fields)


def _incremental_resource(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    start_date: datetime,
    cutoff: datetime,
) -> common.Resource:
    return common.Resource(
        name=object_name,
        key=["/Id"],
        model=ZuoraDocument,
        open=functools.partial(
            _open_incremental, object_name, fields, manager, start_date
        ),
        initial_state=ResourceState(
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            inc=ResourceState.Incremental(cursor=cutoff),
        ),
        initial_config=ResourceConfig(name=object_name, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def _snapshot_resource(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
) -> common.SnapshotResource:
    """Build a full-refresh resource for an object without an UpdatedDate field."""
    return common.SnapshotResource(
        name=object_name,
        model=BaseCSVRow,
        open=functools.partial(_open_snapshot, object_name, fields, manager),
        initial_config=ResourceConfig(name=object_name, interval=timedelta(hours=1)),
    )


async def _build_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    object_names: list[str],
) -> list[common.Resource]:
    """Describe the given objects concurrently and build their capture resources."""
    manager = ExportManager(http, log, config.base_url)

    cutoff = (datetime.now(UTC) - api.LAG).replace(microsecond=0)

    sem = asyncio.Semaphore(_DISCOVER_CONCURRENCY)
    describe_results = await asyncio.gather(*[
        _describe_one(name, config.base_url, http, log, sem)
        for name in object_names
    ])

    resources: list[common.Resource] = []
    for described in describe_results:
        if described is None:
            continue
        if "UpdatedDate" in described.fields:
            resources.append(
                _incremental_resource(
                    described.name, described.fields, manager, config.start_date, cutoff
                )
            )
        else:
            resources.append(_snapshot_resource(described.name, described.fields, manager))

    return resources


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    _setup_http_auth(http, config)
    object_names = await api.discover_object_names(config.base_url, http, log)
    return await _build_resources(log, http, config, object_names)


async def enabled_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bindings: list[common._ResolvableBinding],
) -> list[common.Resource]:
    _setup_http_auth(http, config)
    object_names = [binding.resourceConfig.name for binding in bindings]
    return await _build_resources(log, http, config, object_names)


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> None:
    _setup_http_auth(http, config)
    try:
        await api.discover_object_names(config.base_url, http, log)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                ["Authentication failed. Please check your credentials."]
            )
        raise ValidationError([f"Failed to connect to Zuora: {err.message}."])
    except Exception as err:
        raise ValidationError([f"Failed to connect to Zuora: {err}."])
