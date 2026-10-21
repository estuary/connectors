import pytest

from source_intercom_native.models import parse_api_version
from source_intercom_native.resources import _resolve_snapshot_endpoint


@pytest.mark.parametrize(
    "lower, higher",
    [
        ("2.9", "2.16"),
        ("2.15", "2.16"),
        ("2.16", "3.0"),
    ],
)
def test_parse_api_version_compares_numerically(lower, higher):
    assert parse_api_version(lower) < parse_api_version(higher)


def test_parse_api_version():
    assert parse_api_version("2.16") == (2, 16)


@pytest.mark.parametrize("api_version", ["2.11", "2.15"])
def test_conversation_attributes_endpoint_before_2_16(api_version):
    assert _resolve_snapshot_endpoint(
        "conversation_attributes", "data_attributes", "conversation", api_version
    ) == ("data_attributes", "conversation")


@pytest.mark.parametrize("api_version", ["2.16", "2.20", "3.0"])
def test_conversation_attributes_endpoint_from_2_16(api_version):
    assert _resolve_snapshot_endpoint(
        "conversation_attributes", "data_attributes", "conversation", api_version
    ) == ("conversations/attributes", None)


@pytest.mark.parametrize("name", ["company_attributes", "contact_attributes"])
def test_other_attributes_endpoints_unchanged_on_2_16(name):
    model = name.removesuffix("_attributes")
    assert _resolve_snapshot_endpoint(name, "data_attributes", model, "2.16") == (
        "data_attributes",
        model,
    )
