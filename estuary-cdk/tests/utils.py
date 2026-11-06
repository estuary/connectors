from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
    WebhookResourceConfig,
)
from estuary_cdk.capture.webhook.match import CollectionMatchingSpec, UrlMatch
from estuary_cdk.capture.webhook.resources import (
    WebhookDocument,
    WebhookResource,
    open_webhook_binding,
)


def make_webhook_resource(
    name: str,
    match_rule: CollectionMatchingSpec | None = None,
) -> Resource[WebhookDocument, WebhookResourceConfig, ResourceState]:
    if match_rule is None:
        match_rule = UrlMatch(value="*")
    return WebhookResource(
        name=name,
        open=open_webhook_binding,  # pyright: ignore[reportArgumentType]
        initial_config=WebhookResourceConfig(
            name=name,
            match_rule=match_rule,
        ),
        schema_inference=True,
    )


def make_pull_resource(
    name: str = "pull",
) -> Resource[WebhookDocument, ResourceConfig, ResourceState]:
    return Resource(
        name=name,
        key=["/id"],
        model=WebhookDocument,
        open=lambda *args, **kwargs: None,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=name),
        schema_inference=True,
    )
