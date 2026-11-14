import functools
import logging
from typing import Any

from estuary_cdk.capture import common

from source_zendesk_support_native.models import EndpointConfig
from source_zendesk_support_native.resources import _generate_resources

log = logging.getLogger(__name__)

AFFECTED_STREAMS = {
    "organizations",
    "users",
    "tickets",
    "ticket_audits",
    "ticket_comments",
    "side_conversations",
    "ticket_metrics",
    "ticket_metric_events",
}


def _resources(feature_flags: str) -> dict[str, common.Resource[Any, Any, Any]]:
    config = EndpointConfig.model_validate({
        "subdomain": "example",
        "credentials": {"credentials_title": "Email & API Token", "username": "someone@example.com", "password": "token"},
        "advanced": {"feature_flags": feature_flags},
    })
    config.advanced.resolve_feature_flags(None)
    return {r.name: r for r in _generate_resources(log, None, config)}  # type: ignore[arg-type]


def _builder(resource: common.Resource[Any, Any, Any]) -> str:
    # Each builder's open is a closure defined inside it, so its qualified name
    # names the builder.
    open = resource.open.func if isinstance(resource.open, functools.partial) else resource.open
    return open.__qualname__.split(".")[0]


def test_use_list_endpoints_switches_only_the_affected_streams() -> None:
    exports = _resources("no_use_list_endpoints")
    list_endpoints = _resources("use_list_endpoints")

    assert exports.keys() == list_endpoints.keys()

    switched = {name for name in exports if _builder(exports[name]) != _builder(list_endpoints[name])}
    assert switched == AFFECTED_STREAMS
