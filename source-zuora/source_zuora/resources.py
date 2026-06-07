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

from .api import (
    LAG,
    discover_object_names,
    fetch_changes,
    fetch_object_fields,
    fetch_page,
    fetch_snapshot,
)
from .export_manager import ExportManager
from .models import (
    EndpointConfig,
    TransactionDateDocument,
    UpdatedDateDocument,
    ZuoraDocument,
)


DOCUMENT_MODELS: list[type[ZuoraDocument]] = [
    UpdatedDateDocument,
    TransactionDateDocument,
]


_DISCOVER_SEM = asyncio.Semaphore(10)


def _attach_token_source(http: HTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(
        oauth_spec=OAuth2TokenFlowSpec(
            accessTokenUrlTemplate=f"{config.base_url}/oauth/token",
            accessTokenResponseMap={"access_token": "/access_token"},
        ),
        credentials=config.credentials,
    )


@dataclass(frozen=True)
class DescribedObject:
    """A queryable Zuora object and its exportable field names."""
    name: str
    fields: list[str]


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> None:
    _attach_token_source(http, config)
    try:
        await discover_object_names(config.base_url, http, log)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                ["Authentication failed. Please check your credentials."]
            ) from err
        raise ValidationError([f"Failed to connect to Zuora: {err.message}."]) from err
    except Exception as err:
        raise ValidationError([f"Failed to connect to Zuora: {err}."]) from err


async def _describe_object(
    object_name: str,
    base_url: str,
    http: HTTPMixin,
    log: Logger,
) -> DescribedObject | None:
    async with _DISCOVER_SEM:
        try:
            fields = await fetch_object_fields(base_url, http, log, object_name)
        except Exception as err:
            detail = {"http_code": err.code} if isinstance(err, HTTPError) else {"error": str(err)}
            log.warning("Skipping object: describe failed", {"object": object_name, **detail})
            return None

    if not fields:
        log.warning("Skipping object: no selectable fields", {"object": object_name})
        return None
    return DescribedObject(name=object_name, fields=fields)


def _incremental_resource(
    object_name: str,
    fields: list[str],
    model: type[ZuoraDocument],
    manager: ExportManager,
    start_date: datetime,
    cutoff: datetime,
) -> common.Resource:
    def open(
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
                fetch_changes,
                object_name,
                fields,
                model,
                manager,
            ),
            fetch_page=functools.partial(
                fetch_page,
                object_name,
                fields,
                model,
                manager,
                start_date,
            ),
        )

    return common.Resource(
        name=object_name,
        key=["/Id"],
        model=model,
        open=open,
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
    def open(
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
                fetch_snapshot,
                object_name,
                fields,
                manager,
            ),
        )

    return common.SnapshotResource(
        name=object_name,
        model=BaseCSVRow,
        open=open,
        initial_config=ResourceConfig(name=object_name, interval=timedelta(hours=4)),
    )


async def _build_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    object_names: list[str],
) -> list[common.Resource]:
    manager = ExportManager(http, log, config.base_url)

    cutoff = (datetime.now(UTC) - LAG).replace(microsecond=0)

    describe_results = await asyncio.gather(*[
        _describe_object(name, config.base_url, http, log)
        for name in object_names
    ])

    resources: list[common.Resource] = []
    for described in describe_results:
        if described is None:
            continue
        model = next(
            (m for m in DOCUMENT_MODELS if m.CURSOR_FIELD in described.fields), None
        )
        if model is not None:
            resources.append(
                _incremental_resource(
                    described.name, described.fields, model, manager, config.start_date, cutoff
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
    _attach_token_source(http, config)
    object_names = await discover_object_names(config.base_url, http, log)
    return await _build_resources(log, http, config, object_names)


async def enabled_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bindings: list[common._ResolvableBinding],
) -> list[common.Resource]:
    _attach_token_source(http, config)
    object_names = [binding.resourceConfig.name for binding in bindings]
    return await _build_resources(log, http, config, object_names)



