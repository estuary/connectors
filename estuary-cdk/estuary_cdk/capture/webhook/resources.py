from collections.abc import Sequence
from datetime import UTC, datetime
from typing import ClassVar, Generic, TypeVar, override
from uuid import uuid4
from pydantic import BaseModel, Field
from estuary_cdk.capture.common import (
    BaseDocument,
    BaseResourceConfig,
    Resource,
    ResourceState,
    open_binding,
)
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.webhook.match import (
    CollectionDiscriminatorSpec,
    CollectionMatchingSpec,
    UrlDiscriminator,
    UrlMatch,
)
from estuary_cdk.flow import CaptureBinding


class WebhookDocument(BaseDocument):
    class Meta(BaseDocument.Meta):
        webhookId: str = Field(default_factory=lambda: str(uuid4()))
        receivedAt: str = Field(
            default_factory=lambda: datetime.now(tz=UTC).isoformat()
        )
        headers: dict[str, str]
        reqPath: str
        pathParams: dict[str, str] | None = None
        queryParams: dict[str, str] | None = None

    meta_: Meta = Field(
        default_factory=lambda: WebhookDocument.Meta(op="u", headers={}, reqPath=""),
        alias="_meta",
        description="Document metadata",
    )


_WebhookDocument = TypeVar("_WebhookDocument", bound=WebhookDocument)


class WebhookResourceConfig(BaseResourceConfig):
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]

    name: str = Field(description="Name of this resource")
    match_rule: CollectionMatchingSpec = Field(
        default_factory=lambda: UrlMatch(value="*"),
        description="Matching spec for routing incoming webhooks to this collection",
        discriminator="type",
    )

    @override
    def path(self) -> list[str]:
        return [self.name]


def open_webhook_binding(
    binding: CaptureBinding[WebhookResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_bindings: Sequence[
        tuple[
            CaptureBinding[BaseResourceConfig],
            Resource[_WebhookDocument, BaseResourceConfig, ResourceState],
        ]
    ],
):
    open_binding(
        binding,  # pyright: ignore[reportArgumentType]
        binding_index,
        resource_state,
        task,
    )

    # The webhook server is started here rather than in common.open() to keep
    # webhook-specific coupling (WebhookResourceConfig filtering, aiohttp) out
    # of the shared open path
    if task.stopping.webhook_task is None:
        from estuary_cdk.capture.webhook.server import start_webhook_server

        webhook_bindings = {
            idx: rsc
            for idx, (_, rsc) in enumerate(all_bindings)
            if isinstance(rsc.initial_config, WebhookResourceConfig)
        }
        start_webhook_server(webhook_bindings, task)


CATCH_ALL_DISCRIMINATOR = UrlDiscriminator()


class WebhookCaptureSpec(BaseModel, Generic[_WebhookDocument]):
    name: str = Field(
        default="webhook-data",
        description="Unique name representing the set of captured messages",
    )
    key: list[str] = Field(
        default=["/_meta/webhookId"],
        description="Location of each document's unique identifier",
    )
    discriminator: CollectionDiscriminatorSpec = Field(
        default_factory=UrlDiscriminator,
        description="Strategy for routing incoming webhooks to collections",
        discriminator="type",
    )
    document_model: type[_WebhookDocument] = Field(
        default=WebhookDocument, exclude=True
    )  # pyright: ignore[reportAssignmentType]

    def create_resources(
        self,
    ) -> list[Resource[_WebhookDocument, WebhookResourceConfig, ResourceState]]:
        if self.discriminator == CATCH_ALL_DISCRIMINATOR:
            return [
                Resource(
                    name=self.name,
                    key=self.key,
                    model=self.document_model,
                    open=open_webhook_binding,  # pyright: ignore[reportArgumentType]
                    initial_state=ResourceState(),
                    initial_config=WebhookResourceConfig(
                        name=self.name,
                        match_rule=CATCH_ALL_DISCRIMINATOR.for_value("*"),
                    ),
                    schema_inference=True,
                )
            ]

        return [
            Resource(
                name=f"{self.name}_{rule.display_name}",
                key=["/_meta/webhookId"],
                model=self.document_model,
                open=open_webhook_binding,  # pyright: ignore[reportArgumentType]
                initial_state=ResourceState(),
                initial_config=WebhookResourceConfig(
                    name=f"{self.name}_{rule.display_name}",
                    match_rule=rule,
                ),
                schema_inference=True,
            )
            for rule in self.discriminator.create_match_rules()
        ]
