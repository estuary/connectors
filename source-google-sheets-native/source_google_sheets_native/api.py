from datetime import datetime, timedelta
from decimal import Decimal
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import Iterable, Any
from zoneinfo import ZoneInfo

from .models import (
    NumberType,
    Row,
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
    log: Logger,
    http: HTTPSession,
    spreadsheet_id: str,
    sheet: Sheet,
) -> Iterable[Row]:

    url = f"{API}/v4/spreadsheets/{spreadsheet_id}"
    params = {
        "ranges": [sheet.properties.title],
        "fields": "properties,sheets.properties,sheets.data(rowData.values(effectiveFormat(numberFormat(type)),effectiveValue))",
    }

    spreadsheet = Spreadsheet.model_validate_json(
        await http.request(log, url, params=params)
    )

    if len(spreadsheet.sheets) == 0:
        raise RuntimeError(f"Spreadsheet sheet '{sheet}' was not found")

    sheet = spreadsheet.sheets[0]
    assert sheet.data
    rows = sheet.data[0].rowData

    headers = default_column_headers(sheet.properties.gridProperties.columnCount)

    # Augment with headers from a frozen row, if there is one.
    if sheet.properties.gridProperties.frozenRowCount == 1:
        for ind, column in enumerate(rows[0].values):
            if (ev := column.effectiveValue) and ev.stringValue:
                headers[ind] = ev.stringValue
        rows.pop(0)

    # https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
    user_tz = ZoneInfo(spreadsheet.properties.timeZone)
    lotus_epoch = datetime(1899, 12, 30, tzinfo=user_tz)

    return (
        convert_row(id, headers, row.values, lotus_epoch) for id, row in enumerate(rows)
    )


def convert_row(
    id: int, headers: list[str], columns: list[Sheet.Value], epoch: datetime
) -> Row:
    d: dict[str, Any] = {}

    for ind, column in enumerate(columns):
        if not isinstance((ev := column.effectiveValue), Sheet.EffectiveValue):
            continue

        if isinstance((sv := ev.stringValue), str):
            d[headers[ind]] = sv

        if isinstance((nv := ev.numberValue), Decimal):
            nt = (
                isinstance((ef := column.effectiveFormat), Sheet.EffectiveFormat)
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

    return Row(_meta=Row.Meta(op="u", row_id=id), **d)


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
