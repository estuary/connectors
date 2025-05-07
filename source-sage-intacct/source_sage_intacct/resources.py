import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_changes,
    fetch_page,
)
from .models import (
    DynamicRecordModel,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
)
from .sage import Sage

SUPPORTED_OBJECTS = {
    "CUSTOMER",
    "APTERM",
    "CLASS",
    "DEPARTMENT",
    "EMPLOYEE",
    "GLACCOUNT",
    "LOCATION",
    "TAXDETAIL",
    "VENDOR",
    "TRXCURRENCIES",
    "GLJOURNAL",
    "PROJECT",
    #  "COMPANYPREF",
    #  "TAXSOLUTION",
    #  "TASK",
    #  "ITEM",
}


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    sage = Sage(log, http, config)
    await sage.setup()

    def open(
        obj: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                "current": functools.partial(fetch_changes, obj, sage),
                # TODO: Eventually consistent delayed stream
            },
            fetch_page=functools.partial(fetch_page, obj, sage),
        )

    started_at = datetime.now(tz=UTC)

    return [
        common.Resource(
            name=obj,
            key=["/RECORDNO"],
            model=DynamicRecordModel,
            open=functools.partial(open, obj),
            initial_state=ResourceState(
                inc={
                    "current": ResourceState.Incremental(cursor=started_at),
                },
                backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
            ),
            initial_config=ResourceConfig(name=obj, interval=timedelta(minutes=5)),
            schema_inference=True,
        )
        for obj in SUPPORTED_OBJECTS
    ]
