import json
from datetime import date, timedelta
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, Field

from estuary_cdk.capture import BaseCaptureConnector, Request, response
from estuary_cdk.capture.common import ConnectorState, ResourceConfig, ResourceState
from estuary_cdk.feature_flags import (
    FLAG_DISABLED,
    FLAG_ENABLED,
    FeatureFlagsConfig,
    FlagDefault,
    parse_feature_flags,
)
from estuary_cdk.logger import FlowLogger

CUTOFF = date(2026, 10, 20)


def test_fixed_defaults_ignore_creation_date():
    for created_at in (None, date(2000, 1, 1), date(2999, 1, 1)):
        assert FLAG_ENABLED.resolve(created_at)
        assert not FLAG_DISABLED.resolve(created_at)


@pytest.mark.parametrize(
    "created_at, expected",
    [
        (CUTOFF - timedelta(days=1), False),
        (CUTOFF, True),
        (CUTOFF + timedelta(days=1), True),
    ],
)
def test_date_gated_default(created_at: date, expected: bool):
    gated = FlagDefault.enabled_for_tasks_created_after(CUTOFF)
    assert gated.resolve(created_at) == expected


def test_date_gated_default_for_brand_new_task():
    # A task without a creation date resolves against today.
    assert FlagDefault.enabled_for_tasks_created_after(date(2000, 1, 1)).resolve(None)
    assert not FlagDefault.enabled_for_tasks_created_after(date(2999, 1, 1)).resolve(None)


DEFAULTS = {"on": True, "off": False}


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("", {"on": True, "off": False}),
        ("no_on,off", {"on": False, "off": True}),
        ("new", {"on": True, "off": False, "new": True}),
        ("no_new", {"on": True, "off": False, "new": False}),
        (" off , ,no_ ", {"on": True, "off": True}),
        # The last entry for a flag wins.
        ("off,no_off", {"on": True, "off": False}),
    ],
)
def test_parse_feature_flags(raw: str, expected: dict[str, bool]):
    assert parse_feature_flags(raw, DEFAULTS) == expected


class _Advanced(FeatureFlagsConfig):
    FEATURE_FLAG_DEFAULTS: ClassVar[dict[str, FlagDefault]] = {
        "gated": FlagDefault.enabled_for_tasks_created_after(CUTOFF),
        "gated_since_2000": FlagDefault.enabled_for_tasks_created_after(date(2000, 1, 1)),
        "off": FLAG_DISABLED,
    }


def test_explicit_flags_win_over_date_gated_defaults():
    after_cutoff = _Advanced(feature_flags="no_gated")
    after_cutoff.resolve_feature_flags(CUTOFF)
    assert not after_cutoff.flag("gated")

    before_cutoff = _Advanced(feature_flags="gated")
    before_cutoff.resolve_feature_flags(CUTOFF - timedelta(days=1))
    assert before_cutoff.flag("gated")


def test_flag_reads_fail_loudly():
    advanced = _Advanced(feature_flags="undeclared_but_set")
    with pytest.raises(RuntimeError):
        advanced.flag("gated")

    advanced.resolve_feature_flags(CUTOFF)
    with pytest.raises(KeyError):
        advanced.flag("undeclared_but_set")


@pytest.mark.filterwarnings("ignore:Field name .FEATURE_FLAG_DEFAULTS.")
def test_flag_defaults_are_checked_when_the_class_is_defined():
    with pytest.raises(TypeError):

        class _MissingClassVar(FeatureFlagsConfig):
            FEATURE_FLAG_DEFAULTS: dict[str, FlagDefault] = {"x": FLAG_DISABLED}

    with pytest.raises(ValueError):

        class _NegatedName(FeatureFlagsConfig):
            FEATURE_FLAG_DEFAULTS = {"no_x": FLAG_DISABLED}


class _EndpointConfig(BaseModel):
    advanced: _Advanced = Field(default_factory=_Advanced)


_State = ConnectorState[ResourceState]
_LOG = FlowLogger("test-feature-flags")


class _OpenReached(Exception):
    """Stops handle() before it starts capturing."""


class _Connector(BaseCaptureConnector[_EndpointConfig, ResourceConfig, _State]):
    """Records the flags each handler sees."""

    def __init__(self, updated_config: _EndpointConfig | None = None):
        super().__init__()
        self.updated_config = updated_config
        self.seen: dict[str, bool] | None = None

    @classmethod
    def request_class(cls):
        return Request[_EndpointConfig, ResourceConfig, _State]

    def record(self, config: _EndpointConfig):
        self.seen = {
            name: config.advanced.flag(name)
            for name in _Advanced.FEATURE_FLAG_DEFAULTS
        }

    async def spec(self, log, _):
        raise NotImplementedError

    async def discover(self, log, discover):
        self.record(discover.config)
        return response.Discovered(bindings=[])

    async def validate(self, log, validate):
        self.record(validate.config)
        return response.Validated(bindings=[])

    async def apply(self, log, apply):
        self.record(apply.capture.config)
        return response.Applied(actionDescription="")

    async def open(self, log, open):
        self.record(open.capture.config)
        raise _OpenReached()

    async def update_config(self, log, config):
        if self.updated_config is None:
            return None
        return "updating config", self.updated_config

    async def _encrypt_config(self, log, config):
        return {}

    async def _emit(self, response):
        pass


def _request(rpc: str, created_at: str | None) -> str:
    config: dict[str, Any] = {"advanced": {"feature_flags": ""}}
    dated: dict[str, Any] = {"createdAt": created_at} if created_at else {}
    capture = {
        "name": "acmeCo/capture",
        "connectorType": "IMAGE",
        "config": config,
        "intervalSeconds": 300,
        **dated,
    }

    match rpc:
        case "discover":
            body = {"connectorType": "IMAGE", "config": config, **dated}
        case "validate":
            body = {"name": "acmeCo/capture", "connectorType": "IMAGE", "config": config}
            # Only a task that has been published has a last capture.
            if created_at:
                body["lastCapture"] = {**capture, "config": {"config": config}}
        case "apply":
            body = {"capture": capture, "version": "v1"}
        case "open":
            body = {"capture": capture, "version": "v1", "range": {}, "state": {}}
        case _:
            raise ValueError(rpc)

    return json.dumps({rpc: body})


async def _handle(connector: BaseCaptureConnector, raw: str):
    request = connector.request_class().model_validate_json(raw)
    try:
        await connector.handle(_LOG, request)
    except _OpenReached:
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize("rpc", ["discover", "validate", "apply", "open"])
@pytest.mark.parametrize(
    "created_at, gated", [("2026-10-19", False), ("2026-10-20", True)]
)
async def test_handle_resolves_flags_from_creation_date(
    rpc: str, created_at: str, gated: bool
):
    connector = _Connector()
    await _handle(connector, _request(rpc, created_at))
    assert connector.seen == {"gated": gated, "gated_since_2000": True, "off": False}


@pytest.mark.asyncio
async def test_handle_resolves_unpublished_task_against_today():
    connector = _Connector()
    await _handle(connector, _request("validate", None))
    assert connector.seen is not None
    assert connector.seen["gated_since_2000"]


@pytest.mark.asyncio
async def test_open_resolves_flags_of_updated_config():
    # update_config may replace the config, and the replacement is what the
    # connector reads its flags from.
    connector = _Connector(
        updated_config=_EndpointConfig(advanced=_Advanced(feature_flags="off"))
    )
    await _handle(connector, _request("open", "2026-10-20"))
    assert connector.seen == {"gated": True, "gated_since_2000": True, "off": True}


class _UnflaggedAdvanced(BaseModel):
    feature_flags: str = ""


class _UnflaggedEndpointConfig(BaseModel):
    advanced: _UnflaggedAdvanced = Field(default_factory=_UnflaggedAdvanced)


class _UnflaggedConnector(_Connector):
    @classmethod
    def request_class(cls):
        return Request[_UnflaggedEndpointConfig, ResourceConfig, _State]

    def record(self, config):
        self.seen = {}


@pytest.mark.asyncio
@pytest.mark.parametrize("rpc", ["discover", "validate", "apply", "open"])
async def test_handle_leaves_connectors_without_feature_flags_config_alone(rpc: str):
    # A connector that parses its own feature_flags property, or has no
    # advanced section at all, must keep working untouched.
    connector = _UnflaggedConnector()
    await _handle(connector, _request(rpc, "2026-10-20"))
    assert connector.seen == {}
