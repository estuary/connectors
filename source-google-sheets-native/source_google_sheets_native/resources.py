from datetime import timedelta
from logging import Logger
from typing import AsyncGenerator
import re

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .models import (
    EndpointConfig,
    OAUTH2_SPEC,
    ResourceConfig,
    ResourceState,
    Row,
    Sheet,
)
from .api import (
    fetch_spreadsheet,
    fetch_rows,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    spreadsheet_id = get_spreadsheet_id(config.spreadsheet_url)

    spreadsheet = await fetch_spreadsheet(log, http, spreadsheet_id)
    return [sheet(http, spreadsheet_id, s) for s in spreadsheet.sheets]


def get_spreadsheet_id(url: str):
    m = re.search(r"(/)([-\w]{20,})([/]?)", url)
    if m is not None and m.group(2):
        return m.group(2)
    raise ValidationError([f"invalid spreadsheet URL: {url}"])


def sheet(http: HTTPSession, spreadsheet_id: str, sheet: Sheet):

    async def snapshot(log: Logger) -> AsyncGenerator[Row, None]:
        rows = await fetch_rows(log, http, spreadsheet_id, sheet)
        for row in rows:
            yield row

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot,
            tombstone=Row(_meta=Row.Meta(op="d")),
        )

    return common.Resource(
        name=sheet.properties.title,
        key=["/_meta/row_id"],
        model=Row,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=sheet.properties.title, interval=timedelta(seconds=30)
        ),
        schema_inference=True,
    )
