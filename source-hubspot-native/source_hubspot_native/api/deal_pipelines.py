from logging import Logger

from estuary_cdk.http import HTTPSession

from ..models import (
    DealPipelines,
)
from .shared import (
    HUB,
)


async def fetch_deal_pipelines(log: Logger, http: HTTPSession) -> DealPipelines:
    url = f"{HUB}/crm-pipelines/v1/pipelines/deals"

    return DealPipelines.model_validate_json(await http.request(log, url))
