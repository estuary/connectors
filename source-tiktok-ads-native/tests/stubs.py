"""A stand-in for the CDK's HTTP session that answers like TikTok, shared by the
report and resource tests."""

import asyncio
import json
import logging
import math
from datetime import date
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPError
from multidict import CIMultiDict, CIMultiDictProxy
from source_tiktok_ads_native.api import client as client_module
from source_tiktok_ads_native.api import TikTokClient
from source_tiktok_ads_native.api.reports import QUERY_TOO_LARGE
from source_tiktok_ads_native.models import Campaign


class ReportStub:
    """Stand-in for HTTPSession. `request` answers `oauth2/advertiser/get` with
    every advertiser in `zones` and `unreadable`, and `advertiser/info` with each
    requested advertiser's time zone; like TikTok, it fails the whole request
    with `40001` for an ID it can't read. `request_stream` answers report
    requests and the campaign listing.

    The campaign listing stamps each campaign with its `created` time, if
    given. A report request covering more than `truncate_above` campaigns (an
    unfiltered one covers all of them) is flagged as truncated, from page
    `truncated_from_page` on; a filtered one answers one row per campaign. Each
    of `pages` pages repeats the rows. A dated request spanning more than
    `too_large_above_days` days answers `40067`.

    Signatures mirror HTTPSession positionally, and serving bytes keeps the real
    IncrementalJsonProcessor and Pydantic parsing inside the test."""

    def __init__(
        self,
        zones: dict[str, str],
        rows: list[dict[str, Any]] | None = None,
        campaigns: list[str] | None = None,
        truncate_above: int | None = None,
        too_large_above_days: int | None = None,
        error_code: int = 0,
        pages: int = 1,
        truncated_from_page: int = 1,
        unreadable: set[str] | None = None,
        info_error: tuple[int, str] | None = None,
        oauth_http_status: int = 0,
        created: dict[str, str] | None = None,
    ):
        self.zones = zones
        self.rows = rows or []
        self.campaigns = campaigns or []
        self.created = created or {}
        self.truncate_above = truncate_above
        self.campaign_listings = 0
        self.info_batches: list[int] = []
        self.too_large_above_days = too_large_above_days
        self.error_code = error_code
        self.pages = pages
        self.truncated_from_page = truncated_from_page
        self.report_requests: list[dict[str, Any]] = []
        self.oauth_params: dict[str, Any] | None = None
        self.unreadable = unreadable or set()
        self.info_error = info_error
        self.oauth_http_status = oauth_http_status

    async def request(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bytes:
        params = params or {}
        # Yield like a real request, so concurrent callers interleave.
        await asyncio.sleep(0)
        if url.endswith("oauth2/advertiser/get/"):
            self.oauth_params = params
            if self.oauth_http_status:
                # As the CDK reports it: the request URL, query string included.
                query = "&".join(f"{k}={v}" for k, v in params.items())
                raise HTTPError(
                    f"Encountered HTTP error status {self.oauth_http_status} which cannot be retried.\nURL: {url}?{query}\nResponse:\n{{}}",
                    self.oauth_http_status,
                )
            listed =[{"advertiser_id": i} for i in [*self.zones, *self.unreadable]]
            return json.dumps({"code": 0, "message": "OK", "data": {"list": listed}}).encode()

        ids = json.loads(params["advertiser_ids"])
        self.info_batches.append(len(ids))
        if self.info_error:
            code, message = self.info_error
            return json.dumps({"code": code, "message": message, "data": {}}).encode()
        unknown = [i for i in ids if i not in self.zones or i in self.unreadable]
        if unknown:
            message = f"The advertiser {unknown[0]} doesn't exist or has been deleted."
            return json.dumps({"code": 40001, "message": message, "data": {}}).encode()
        accounts = [
            {
                "advertiser_id": i,
                "create_time": 1791482410,
                "timezone": "Etc/GMT+8",
                "display_timezone": self.zones[i],
            }
            for i in ids
        ]
        return json.dumps({"code": 0, "message": "OK", "data": {"list": accounts}}).encode()

    async def request_stream(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        params = params or {}
        if url.endswith(Campaign.PATH):
            return self._campaign_listing(params)
        self.report_requests.append(params)
        campaign_ids = campaign_filter(params)

        code = self.error_code
        if self.too_large_above_days is not None and "start_date" in params:
            days = (
                date.fromisoformat(params["end_date"])
                - date.fromisoformat(params["start_date"])
            ).days + 1
            if days > self.too_large_above_days:
                code = QUERY_TOO_LARGE

        if code:
            payload: dict[str, Any] = {"code": code, "message": "bad", "data": {}}
        else:
            rows = self.rows
            if campaign_ids is not None:
                rows = [
                    {
                        "dimensions": {"ad_id": f"ad-{c}", "stat_time_day": "2026-10-12 00:00:00"},
                        "metrics": {"spend": "1"},
                    }
                    for c in campaign_ids
                ]
            payload = {
                "code": 0,
                "message": "OK",
                "data": {
                    "list": rows,
                    "page_info": {"page": params["page"], "total_page": self.pages},
                },
            }
        headers = CIMultiDict()
        covered = len(self.campaigns) if campaign_ids is None else len(campaign_ids)
        if (
            self.truncate_above is not None
            and covered > self.truncate_above
            and params["page"] >= self.truncated_from_page
        ):
            headers["x-tt-ads-throttle"] = "Number of ad ids is equal or more than 20k"
        return CIMultiDictProxy(headers), _body(payload)

    def _campaign_listing(self, params: dict[str, Any]) -> tuple[Any, Any]:
        self.campaign_listings += 1
        buying_types = json.loads(params["filtering"])["buying_types"]
        stamp = "2026-10-01 00:00:00"
        listed = [
            {
                "advertiser_id": params["advertiser_id"],
                "campaign_id": c,
                "create_time": self.created.get(c, stamp),
                "modify_time": stamp,
            }
            for c in self.campaigns
            if "AUCTION" in buying_types
        ]
        payload = {
            "code": 0,
            "message": "OK",
            "data": {"list": listed, "page_info": {"page": 1, "total_page": 1}},
        }
        return CIMultiDictProxy(CIMultiDict()), _body(payload)


def _body(payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode()

    async def gen() -> AsyncGenerator[bytes, None]:
        yield body

    return gen


def campaign_filter(params: dict[str, Any]) -> list[str] | None:
    """The campaign IDs a report request is filtered to, or None."""
    for f in json.loads(params.get("filtering", "[]")):
        if f["field_name"] == "campaign_ids":
            return json.loads(f["filter_value"])
    return None


def stub_client(http: Any) -> TikTokClient:
    """An unpaced client over a stub session."""
    return TikTokClient(http, client_module.API, math.inf)


