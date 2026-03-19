import pytest

from estuary_cdk.capture.webhook import (
    HeaderDiscriminator,
)
from estuary_cdk.capture.webhook.match import (
    HeaderMatch,
    UrlMatch,
)
from estuary_cdk.capture.webhook.server import (
    GENERIC_WEBHOOK_RESOURCE,
    WebhookCaptureSpec,
    WebhookResourceConfig,
)


class TestWebhookResourceConfig:
    def test_default_match_rule(self):
        cfg = WebhookResourceConfig(name="test")
        assert isinstance(cfg.match_rule, UrlMatch)
        assert cfg.match_rule.value == "*"

    def test_path(self):
        assert WebhookResourceConfig(name="my-hook").path() == ["my-hook"]

    def test_custom_match_rule(self):
        rule = UrlMatch(value="/custom")
        cfg = WebhookResourceConfig(name="test", match_rule=rule)
        assert cfg.match_rule.value == "/custom"


class TestWebhookCaptureSpec:
    def test_no_known_values_returns_generic(self):
        spec = WebhookCaptureSpec(name="test")
        resources = spec.create_resources()
        assert len(resources) == 1
        assert resources[0] is GENERIC_WEBHOOK_RESOURCE

    def test_with_known_values(self):
        spec = WebhookCaptureSpec(
            name="events",
            discriminator=HeaderDiscriminator(
                key="X-Event", known_values={"push", "pull"}
            ),
        )
        resources = spec.create_resources()
        assert len(resources) == 2
        names = {r.name for r in resources}
        assert names == {"events_push", "events_pull"}
        for r in resources:
            assert r.key == ["/_meta/webhookId"]
            assert r.schema_inference is True
            assert isinstance(r.initial_config.match_rule, HeaderMatch)
            assert r.initial_config.match_rule.key == "X-Event"


@pytest.mark.asyncio
async def test_run_webhook_server():
    pytest.skip(
        "_run_webhook_server is incomplete; test deferred until implementation stabilises"
    )


def test_start_webhook_server():
    pytest.skip(
        "start_webhook_server is incomplete; test deferred until implementation stabilises"
    )
