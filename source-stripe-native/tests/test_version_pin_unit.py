from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from multidict import CIMultiDict
from pydantic import ValidationError

from source_stripe_native import Connector
from source_stripe_native.api import fetch_api_version
from source_stripe_native.models import (
    Charges,
    EndpointConfig,
    SubscriptionItems,
    Subscriptions,
    SUBSCRIPTIONS_MIN_API_VERSION,
)
from source_stripe_native.resources import _filter_out_streams_below_min_api_version


async def _empty_body():
    # An empty async generator: fetch_api_version drains the body to release the
    # connection, so the mock just needs to yield nothing.
    return
    yield


def _http_returning(headers: CIMultiDict) -> MagicMock:
    http = MagicMock()
    http.request_stream = AsyncMock(return_value=(headers, _empty_body))
    return http


def _resource(model: type) -> SimpleNamespace:
    # Stand-in for estuary_cdk Resource; the helper only reads `name` and `model`.
    return SimpleNamespace(name=model.NAME, model=model)


class TestFetchApiVersion:
    @pytest.mark.asyncio
    async def test_returns_header_value_case_insensitively(self):
        # Capitalized on purpose — the lookup must be case-insensitive.
        http = _http_returning(CIMultiDict({"Stripe-Version": "2024-06-20"}))
        assert await fetch_api_version(http, MagicMock()) == "2024-06-20"

    @pytest.mark.asyncio
    async def test_raises_when_header_missing(self):
        http = _http_returning(CIMultiDict())
        with pytest.raises(RuntimeError, match="stripe-version"):
            _ = await fetch_api_version(http, MagicMock())


class TestMinimumApiVersionDeclared:
    def test_subscription_streams_declare_the_minimum(self):
        assert Subscriptions.MINIMUM_API_VERSION == SUBSCRIPTIONS_MIN_API_VERSION
        assert SubscriptionItems.MINIMUM_API_VERSION == SUBSCRIPTIONS_MIN_API_VERSION
        assert SUBSCRIPTIONS_MIN_API_VERSION == "2016-07-06"

    def test_other_streams_inherit_the_none_default(self):
        # Streams that don't override it inherit BaseStripeObject's "no minimum".
        assert Charges.MINIMUM_API_VERSION is None


class TestRemoveStreamsBelowMinApiVersion:
    def test_removes_stream_when_effective_version_is_older(self):
        for version in ("2014-10-31", "2015-06-15", "2016-07-05"):
            kept = _filter_out_streams_below_min_api_version(
                [_resource(Subscriptions)], version, MagicMock()
            )
            assert kept == [], version

    def test_keeps_stream_when_effective_version_meets_minimum(self):
        for version in ("2016-07-06", "2019-03-14", "2026-05-27.dahlia"):
            resource = _resource(Subscriptions)
            kept = _filter_out_streams_below_min_api_version(
                [resource], version, MagicMock()
            )
            assert kept == [resource], version

    def test_streams_without_a_minimum_are_kept(self):
        resource = _resource(Charges)
        kept = _filter_out_streams_below_min_api_version(
            [resource], "2010-01-01", MagicMock()
        )
        assert kept == [resource]


class TestWithApiVersion:
    """The header-merge that `_request_stream` applies to every call."""

    def test_unpinned_passes_headers_through_unchanged(self):
        connector = Connector()
        assert connector.pinned_api_version is None
        assert connector._with_api_version(None) is None
        existing = {"Stripe-Account": "acct_1"}
        assert connector._with_api_version(existing) == existing

    def test_pinned_injects_version_without_dropping_existing_headers(self):
        connector = Connector()
        connector.pinned_api_version = "2024-06-20"
        assert connector._with_api_version(None) == {"Stripe-Version": "2024-06-20"}
        assert connector._with_api_version({"Stripe-Account": "acct_1"}) == {
            "Stripe-Account": "acct_1",
            "Stripe-Version": "2024-06-20",
        }


class TestApiVersionConfigValidation:
    """The override is validated at config-parse time so a malformed value fails
    loudly instead of silently mis-sorting in the disable-below-floor gate."""

    def test_accepts_blank_and_well_formed_versions(self):
        for value in ("", "2024-06-20", "2026-05-27.dahlia"):
            assert EndpointConfig.Advanced(api_version=value).api_version == value

    def test_rejects_malformed_versions(self):
        for value in ("2024-6-20", "latest", "2016", "v2024-06-20", "2024/06/20"):
            with pytest.raises(ValidationError):
                EndpointConfig.Advanced(api_version=value)
