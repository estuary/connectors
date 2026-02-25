from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    Owner,
    PageResult,
)
from .shared import (
    HUB,
)


async def fetch_owners(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Owner, None]:
    url = f"{HUB}/crm/v3/owners"
    after: str | None = None

    input: dict[str, Any] = {
        "limit": 500,
    }

    while True:
        if after:
            input["after"] = after

        result = PageResult[Owner].model_validate_json(
            await http.request(log, url, method="GET", params=input)
        )

        for owner in result.results:
            yield owner

        if not result.paging:
            break

        after = result.paging.next.after
