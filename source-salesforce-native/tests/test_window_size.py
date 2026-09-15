from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from source_salesforce_native.models import (
    EndpointConfig,
    WindowSizeAsInterval,
    WindowSizeInDays,
)


def _endpoint_config(window_size: Any) -> EndpointConfig:
    # Minimal valid endpoint config; only advanced.window_size matters here.
    return EndpointConfig.model_validate(
        {
            "credentials": {
                "credentials_title": "Username, Password, & Security Token",
                "username": "svc@example.com",
                "password": "pw",
                "security_token": "tok",
            },
            "advanced": {"window_size": window_size},
        }
    )


@pytest.mark.parametrize(
    "window_size, expected",
    [
        # The UI only sets a union's hidden discriminator for required fields,
        # and window_size is optional, so a window set at capture creation time
        # arrives untagged.
        ({"interval": "PT12H"}, WindowSizeAsInterval(interval=timedelta(hours=12))),
        ({"days": 30}, WindowSizeInDays(days=30)),
        # Configs written before window_size became a union stored a bare count of days.
        (30, WindowSizeInDays(days=30)),
        # Fully tagged payloads keep working.
        (
            {"window_type": "interval", "interval": "PT12H"},
            WindowSizeAsInterval(interval=timedelta(hours=12)),
        ),
        ({"window_type": "days", "days": 7}, WindowSizeInDays(days=7)),
    ],
)
def test_window_size_accepts_untagged_and_tagged_payloads(
    window_size: Any, expected: WindowSizeAsInterval | WindowSizeInDays
) -> None:
    assert _endpoint_config(window_size).advanced.window_size == expected


def test_window_size_defaults_when_absent() -> None:
    assert EndpointConfig.Advanced().window_size == WindowSizeInDays(days=18250)


@pytest.mark.parametrize(
    "window_size",
    [
        # Neither variant's value key is present, so there's nothing to infer a tag from and the
        # config should be rejected rather than silently defaulted.
        {},
        {"bogus": 1},
        # An explicit tag that doesn't match the payload stays an error.
        {"window_type": "days", "interval": "PT12H"},
        # Variant constraints still apply: intervals must exceed one minute, days must be positive.
        {"interval": "PT30S"},
        {"days": 0},
    ],
)
def test_window_size_rejects_unresolvable_payloads(window_size: Any) -> None:
    with pytest.raises(ValidationError):
        _ = _endpoint_config(window_size)
