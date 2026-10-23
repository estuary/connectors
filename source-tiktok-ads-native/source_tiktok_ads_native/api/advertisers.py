import asyncio
import json
from collections.abc import AsyncGenerator
from logging import Logger
from zoneinfo import ZoneInfo

from estuary_cdk.http import HTTPError

from ..models import (
    Advertiser,
    AdvertiserBatch,
    AdvertiserList,
)
from .client import TikTokAPIError, TikTokClient, get_data


async def list_authorized_advertiser_ids(
    client: TikTokClient,
    app_id: str,
    secret: str,
    log: Logger,
) -> list[str]:
    """Return the IDs of every advertiser the access token is authorized for."""
    try:
        data = await get_data(
            client,
            "oauth2/advertiser/get/",
            {"app_id": app_id, "secret": secret},
            AdvertiserList,
            log,
        )
    except HTTPError as err:
        # TikTok takes the app secret as a query parameter, and the CDK quotes
        # the request URL in its HTTP errors, so the original error would carry
        # Estuary's app secret into logs and validation messages.
        raise HTTPError(
            f"Encountered HTTP error status {err.code} listing the authorized advertisers.",
            err.code,
        ) from None
    return [advertiser.advertiser_id for advertiser in data.advertisers]


class NoAdvertisersError(RuntimeError):
    pass


async def resolve_advertiser_ids(
    client: TikTokClient,
    configured_ids: list[str],
    app_id: str | None,
    secret: str | None,
    log: Logger,
) -> list[str]:
    """The advertisers a capture reads: the configured IDs, or else every
    advertiser the token is authorized for, which takes the app ID and secret.
    Raises NoAdvertisersError for none, since capturing no advertisers would
    empty the Advertisers collection."""
    if configured_ids:
        return configured_ids
    # Validation requires configured IDs when there is no app ID.
    assert app_id is not None and secret is not None
    ids = await list_authorized_advertiser_ids(client, app_id, secret, log)
    if not ids:
        raise NoAdvertisersError(
            "The authorized TikTok account has no advertisers. Authorize an account with access to at least one advertiser."
        )
    return ids


class AdvertiserAccounts:
    """Every advertiser's account details, read for all of them at once on first
    use and then fixed for the life of the connector process: each one's report
    time zone, and, at start-up, proof that TikTok can still read it."""

    def __init__(self, client: TikTokClient, advertiser_ids: list[str]):
        self._client = client
        self._advertiser_ids = advertiser_ids
        self._zones: dict[str, ZoneInfo] | None = None
        self._lock = asyncio.Lock()

    async def _all_zones(self, log: Logger) -> dict[str, ZoneInfo]:
        async with self._lock:
            if self._zones is None:
                accounts = await get_advertisers(
                    self._client, self._advertiser_ids, log
                )
                self._zones = {a.advertiser_id: a.zone() for a in accounts}
            return self._zones

    async def check(self, log: Logger) -> None:
        """Raise InaccessibleAdvertiserError, with advice, for an advertiser
        TikTok can't read."""
        await self._all_zones(log)

    async def zone(self, advertiser_id: str, log: Logger) -> ZoneInfo:
        zones = await self._all_zones(log)
        if advertiser_id not in zones:
            raise RuntimeError(
                f"TikTok returned no account details for advertiser {advertiser_id}. "
                "Remove it from Advertiser IDs or, if that field is blank, enter the IDs of the advertisers to capture without it."
            )
        return zones[advertiser_id]


# `advertiser/info` rejects requests naming more than 100 advertisers. For one it
# can't read it fails the whole request, naming the advertiser, with 40001:
# TikTok's code for any missing permission.
ADVERTISER_INFO_BATCH_SIZE = 100
NO_PERMISSION = 40001


class InaccessibleAdvertiserError(TikTokAPIError):
    """TikTok can't read one of the requested advertisers' details."""


async def get_advertisers(
    client: TikTokClient,
    advertiser_ids: list[str],
    log: Logger,
) -> list[Advertiser]:
    """Account details for `advertiser_ids`, in advertiser id order."""
    ids = sorted(advertiser_ids)
    advertisers: list[Advertiser] = []
    for start in range(0, len(ids), ADVERTISER_INFO_BATCH_SIZE):
        batch = ids[start : start + ADVERTISER_INFO_BATCH_SIZE]
        try:
            data = await get_data(
                client,
                "advertiser/info/",
                {"advertiser_ids": json.dumps(batch)},
                AdvertiserBatch,
                log,
            )
        except TikTokAPIError as err:
            if err.code != NO_PERMISSION or not any(i in err.message for i in batch):
                raise
            raise InaccessibleAdvertiserError(
                err.code,
                f"{err.message} Remove it from Advertiser IDs or, if that field is blank, enter the IDs of the advertisers to capture without it.",
            ) from err
        advertisers.extend(data.advertisers)
    return sorted(advertisers, key=lambda a: a.advertiser_id)


async def snapshot_advertisers(
    client: TikTokClient,
    advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[Advertiser, None]:
    """Every advertiser in scope, in advertiser id order so that an unchanged set
    of accounts produces an identical snapshot."""
    for advertiser in await get_advertisers(client, advertiser_ids, log):
        yield advertiser
