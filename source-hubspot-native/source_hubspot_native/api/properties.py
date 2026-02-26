from logging import Logger

from estuary_cdk.http import HTTPSession

from ..models import (
    Properties,
)
from .shared import HUB


properties_cache: dict[str, Properties] = {}


async def fetch_properties(
    log: Logger, http: HTTPSession, object_name: str
) -> Properties:
    if object_name in properties_cache:
        return properties_cache[object_name]

    url = f"{HUB}/crm/v3/properties/{object_name}"
    properties_cache[object_name] = Properties.model_validate_json(
        await http.request(log, url)
    )
    for p in properties_cache[object_name].results:
        p.hubspotObject = object_name

    return properties_cache[object_name]
