import logging
from typing import Any

from source_facebook_marketing_native.client import (
    FacebookAPIClient,
    FacebookRequestParams,
)
from source_facebook_marketing_native.models import Campaigns


class ReducingHTTPSession:
    def __init__(self) -> None:
        self.initial_limits: list[int] = []

    async def request(
        self,
        log: logging.Logger,
        url: str,
        params: dict[str, Any],
        **kwargs: Any,
    ) -> bytes:
        self.initial_limits.append(params["limit"])

        if len(self.initial_limits) == 1:
            should_retry = kwargs["should_retry"]
            assert should_retry(
                500,
                {},
                b'{"error":{"message":"Please reduce the amount of data requested"}}',
                1,
            )

        return b'{"data":[]}'


async def test_reduced_page_size_is_reused_for_later_request_to_same_url() -> None:
    http = ReducingHTTPSession()
    client = FacebookAPIClient(
        http=http,  # type: ignore[arg-type]
        log=logging.getLogger("test_reduced_page_size"),
    )
    adapter = client._build_adapter(Campaigns)
    url = client._build_url(Campaigns, "account")

    await client._fetch_resource_data(
        Campaigns,
        adapter,
        url,
        FacebookRequestParams(fields=Campaigns.fields),
        include_deleted=False,
    )
    await client._fetch_resource_data(
        Campaigns,
        adapter,
        url,
        FacebookRequestParams(fields=Campaigns.fields),
        include_deleted=False,
    )

    assert http.initial_limits == [25, 12]
