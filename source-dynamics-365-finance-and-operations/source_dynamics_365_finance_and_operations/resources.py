from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin

from .models import (
    BaseTable,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    tables_from_model_dot_json
)

from .adls_gen2_client import ADLSGen2Client
from .api import fetch_model_dot_json, fetch_changes


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    adls_client = ADLSGen2Client(
        log=log,
        account_name=config.account_name,
        filesystem=config.filesystem,
        sas_token=config.credentials.sas_token,
        http=http,
    )

    try:
        async for _ in adls_client.list_paths():
            break
    except Exception as e:
        msg = f"Encountered error while validating credentials. Please confirm provided credentials are correct. Error: {e}"
        raise ValidationError([msg])


def resources(
        log: Logger, config: EndpointConfig, adls_client: ADLSGen2Client, tables: list[type[BaseTable]],
) -> list[common.Resource]:

    def open(
        model: type[BaseTable],
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
            fetch_changes=functools.partial(
                fetch_changes,
                adls_client,
                model.name,
            )
        )


    return [
        common.Resource(
            name=table.name,
            key=sorted(["/Id"]),
            model=table,
            open=functools.partial(open, table),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=EPOCH),
            ),
            initial_config=ResourceConfig(
                name=table.name, interval=timedelta(days=1)
            ),
            schema_inference=True,
        )
        for table in tables
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    adls_client = ADLSGen2Client(
        log=log,
        account_name=config.account_name,
        filesystem=config.filesystem,
        sas_token=config.credentials.sas_token,
        http=http,
    )

    tables = tables_from_model_dot_json(
        await fetch_model_dot_json(adls_client)
    )

    return resources(log, config, adls_client, tables)
