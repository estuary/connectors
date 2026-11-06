from unittest.mock import MagicMock

from estuary_cdk.capture import common
from estuary_cdk.capture.common import WebhookResourceConfig
from estuary_cdk.capture.webhook.match import (
    HeaderDiscriminator,
    HeaderMatch,
    UrlMatch,
)
from estuary_cdk.capture.webhook.resources import WebhookCaptureSpec
from tests.utils import make_pull_resource, make_webhook_resource


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
        assert resources[0].name == "test"

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


class TestExplicitAcknowledgements:
    def test_no_webhook_bindings_disables_explicit_acks(self):
        opened, _ = common.open(
            MagicMock(),
            [(MagicMock(), make_pull_resource())],
        )
        assert opened.explicitAcknowledgements is False

    def test_at_least_one_webhook_binding_enables_explicit_acks(self):
        opened, _ = common.open(
            MagicMock(),
            [
                (MagicMock(), make_pull_resource()),
                (MagicMock(), make_webhook_resource("hook")),
            ],
        )
        assert opened.explicitAcknowledgements is True

    def test_empty_bindings_disables_explicit_acks(self):
        opened, _ = common.open(MagicMock(), [])
        assert opened.explicitAcknowledgements is False
