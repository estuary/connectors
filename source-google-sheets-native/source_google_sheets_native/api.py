from datetime import datetime, timedelta
from decimal import Decimal
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import AsyncGenerator, Any
from zoneinfo import ZoneInfo

from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    NumberType,
    Row,
    RowData,
    Sheet,
    Spreadsheet,
)

API = "https://sheets.googleapis.com"


async def fetch_spreadsheet(
    log: Logger,
    http: HTTPSession,
    spreadsheet_id: str,
) -> Spreadsheet:
    url = f"{API}/v4/spreadsheets/{spreadsheet_id}"

    return Spreadsheet.model_validate_json(await http.request(log, url))


async def fetch_rows(
    http: HTTPSession,
    spreadsheet_id: str,
    sheet: Sheet,
    log: Logger,
) -> AsyncGenerator[Row, None]:

    url = f"{API}/v4/spreadsheets/{spreadsheet_id}"
    params = {
        "ranges": [sheet.properties.title],
        "fields": "properties,sheets.properties",
    }

    spreadsheet = Spreadsheet.model_validate_json(
        await http.request(log, url, params=params)
    )

    if len(spreadsheet.sheets) == 0:
        raise RuntimeError(f"Spreadsheet sheet '{sheet}' was not found")

    sheet = spreadsheet.sheets[0]
    headers = default_column_headers(sheet.properties.gridProperties.columnCount)
    headers_from_frozen = sheet.properties.gridProperties.frozenRowCount == 1

    # https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
    user_tz = ZoneInfo(spreadsheet.properties.timeZone)
    lotus_epoch = datetime(1899, 12, 30, tzinfo=user_tz)

    params["fields"] = "sheets.data(rowData.values(effectiveFormat(numberFormat(type)),effectiveValue))"

    _, body = await http.request_stream(log, url, params=params)

    async for row in IncrementalJsonProcessor(
        body(),
        "sheets.item.data.item.rowData.item",
        RowData,
    ):
        if headers_from_frozen:
            assert row.values is not None
            for ind, column in enumerate(row.values):
                if (ev := column.effectiveValue):
                    if ev.stringValue:
                        headers[ind] = ev.stringValue
                    elif ev.numberValue:
                        headers[ind] = str(ev.numberValue)
            headers_from_frozen = False
            continue

        if (not row.values) or all(not v.effectiveValue for v in row.values):
            continue

        yield convert_row(headers, row.values, lotus_epoch)


def convert_row(
    headers: list[str], columns: list[RowData.Value], epoch: datetime
) -> Row:
    d: dict[str, Any] = {}

    for ind, column in enumerate(columns):
        if not isinstance((ev := column.effectiveValue), RowData.EffectiveValue):
            continue

        if isinstance((sv := ev.stringValue), str):
            d[headers[ind]] = sv

        if isinstance((bv := ev.boolValue), bool):
            d[headers[ind]] = bv

        if isinstance((nv := ev.numberValue), Decimal):
            nt = (
                isinstance((ef := column.effectiveFormat), RowData.EffectiveFormat)
                and ef.numberFormat.type
            )

            if nt == NumberType.DATE_TIME:
                days, partial_day = divmod(nv, 1)
                d[headers[ind]] = epoch + timedelta(
                    days=int(days), seconds=float(partial_day * 86400)
                )
            elif nt == NumberType.DATE:
                days, partial_day = divmod(nv, 1)
                d[headers[ind]] = (epoch + timedelta(days=int(days))).date()
            else:
                d[headers[ind]] = nv

    return Row(**d)


def default_column_headers(n: int) -> list[str]:
    """
    Generates a list of Google Sheets column identifiers (A, B, C, ..., AA, AB, ...) for a given number of columns.
    """
    columns = []
    for i in range(1, n + 1):
        column = ""
        while i > 0:
            i, remainder = divmod(i - 1, 26)
            column = chr(65 + remainder) + column
        columns.append(column)
    return columns
