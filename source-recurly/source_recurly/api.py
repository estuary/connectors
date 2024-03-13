from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Iterable

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import PageCursor


from .models import Account

async def fetch_accounts(
    http: HTTPSession,
    limit: int,
    log: Logger,
    page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    assert isinstance(log_cursor, datetime)

    url = f"https://v3.recurly.com/accounts"
    params = {
        "limit": 100
    }

    result = Account.model_validate_json(
        await http.request(log, url, params=params)
    )

    return (
        (str(r.id))
        for r in result.results
    ), result.hasMore and result.offset

