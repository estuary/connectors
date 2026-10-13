import asyncio
from datetime import date
from logging import getLogger

import pytest
from botocore.exceptions import ClientError
from estuary_cdk.flow import ValidationError

from source_aws_cost_explorer import api, resources
from source_aws_cost_explorer.models import EndpointConfig

from .fakes import CREDENTIALS, install_cost_explorer


def test_credentials_schema():
    schema = EndpointConfig.model_json_schema()
    assert (
        schema["properties"]["credentials"]["discriminator"]["propertyName"]
        == "credentials_title"
    )
    fields = schema["$defs"]["AWSAccessKey"]["properties"]
    for name in ("access_key_id", "secret_access_key", "session_token"):
        assert fields[name]["secret"] is True


def test_query_uses_explicit_credentials(monkeypatch):
    fake = install_cost_explorer(
        monkeypatch,
        [
            {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2026-09-01", "End": "2026-09-02"},
                        "Estimated": True,
                        "Groups": [],
                        "Total": {
                            name: {"Amount": "0", "Unit": "USD"}
                            for name in api.COST_METRICS
                        },
                    }
                ]
            }
        ],
    )
    credentials = CREDENTIALS.model_copy(update={"session_token": "test-token"})
    result = asyncio.run(
        api.query_cost_and_usage(credentials, date(2026, 9, 1), date(2026, 9, 2))
    )
    assert fake.client_options["aws_access_key_id"] == "test-key"
    assert fake.client_options["aws_secret_access_key"] == "test-secret"
    assert fake.client_options["aws_session_token"] == "test-token"
    assert fake.client_options["region_name"] == "us-east-1"
    assert fake.calls == [
        {
            "TimePeriod": {"Start": "2026-09-01", "End": "2026-09-02"},
            "Granularity": "DAILY",
            "Metrics": list(api.COST_METRICS),
        }
    ]
    assert result[0].ResultsByTime[0].TimePeriod.Start == "2026-09-01"


@pytest.mark.parametrize(
    "code,expected",
    [
        ("AccessDeniedException", ValidationError),
        ("UnrecognizedClientException", ValidationError),
        ("InvalidSignatureException", ValidationError),
        ("ExpiredTokenException", ValidationError),
        ("DataUnavailableException", ValidationError),
        ("OptInRequired", ValidationError),
        ("LimitExceededException", ClientError),
    ],
)
def test_validation_errors(monkeypatch, code, expected):
    async def reject(*_):
        raise ClientError(
            {"Error": {"Code": code, "Message": "test rejection"}}, "GetCostAndUsage"
        )

    monkeypatch.setattr(resources, "query_cost_and_usage", reject)
    config = EndpointConfig(credentials=CREDENTIALS)
    with pytest.raises(expected):
        asyncio.run(resources.validate_credentials(getLogger(), None, config))
