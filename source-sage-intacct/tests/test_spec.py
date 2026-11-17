from datetime import date

import pytest
from estuary_cdk.capture.common import ResourceConfigWithSchedule
from source_sage_intacct.models import EndpointConfig


def test_advanced_config_has_feature_flags():
    advanced = EndpointConfig.model_json_schema()["$defs"]["Advanced"]["properties"]

    assert advanced["feature_flags"]["default"] == ""
    assert "include_company_id_in_documents" in advanced


def test_resource_config_has_an_optional_schedule():
    schema = ResourceConfigWithSchedule.model_json_schema()

    assert schema["properties"]["schedule"]["default"] == ""
    assert schema["required"] == ["name"]


@pytest.mark.parametrize(
    "created_at,emit",
    [
        # An older task sends no signals until support sets the flag.
        (date(2026, 11, 30), False),
        (date(2026, 12, 1), True),
    ],
)
def test_backfill_signal_flag_defaults(created_at: date, emit: bool):
    flags = EndpointConfig.Advanced().resolve_feature_flags(created_at)

    assert flags == {
        "emit_backfill_signals": emit,
        "ignore_config_for_backfill_signals": False,
    }


def test_emit_backfill_signals_can_be_set_for_an_older_task():
    advanced = EndpointConfig.Advanced(feature_flags="emit_backfill_signals")

    assert advanced.resolve_feature_flags(date(2025, 1, 1))["emit_backfill_signals"]


def test_schedule_defaults_to_none():
    assert ResourceConfigWithSchedule(name="GLBATCH").schedule == ""
