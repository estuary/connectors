from __future__ import annotations

import asyncio
import traceback
from abc import ABCMeta, abstractmethod
from collections.abc import Generator, Mapping
from itertools import combinations
from typing import Annotated, Literal, TypeVar

from aiohttp import web
from aiohttp.web_urldispatcher import DynamicResource
from pydantic import BaseModel, model_validator
from pydantic.fields import Field

from estuary_cdk.capture.common import (
    CaptureBinding,
    ConnectorState,
    Resource,
    ResourceConfig,
    ResourceState,
    Task,
    WebhookDocument,
    WebhookResourceConfig,
    open_binding,
)
from estuary_cdk.pydantic_polyfill import JsonValue

_WebhookDocument = TypeVar("_WebhookDocument", bound=WebhookDocument)
_ResourceConfig = TypeVar("_ResourceConfig", bound=ResourceConfig)
_ResourceState = TypeVar("_ResourceState", bound=ResourceState)


def _open_webhook_binding(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_bindings,
):
    open_binding(binding, binding_index, resource_state, task)


# ------------------------------------------------------------------------------
# Match types: concrete match rules used for routing incoming webhooks
# to resources.
#
# CollectionMatchingSpec = UrlMatch | HeaderMatch | BodyMatch
# ------------------------------------------------------------------------------


class UrlMatch(BaseModel):
    type: Literal["url"] = "url"
    value: str

    @model_validator(mode="after")
    def _validate_url_pattern(self) -> UrlMatch:
        if self.value != "*":
            # Defer to aiohttp's path parsing logic
            _ = DynamicResource(self.value)
        return self

    @staticmethod
    def _is_placeholder_segment(segment: str) -> bool:
        return segment.startswith("{") and segment.endswith("}")

    def list_compatibility_errors(self, other: CollectionMatchingSpec) -> str | None:
        if not isinstance(other, type(self)):
            return None

        if self.value == "*" and other.value == "*":
            return "The '*' wildcard can only be used in one UrlMatch definition"
        if self.value == other.value:
            return f"Only one UrlMatch can claim the {self.value!r} discriminator value"
        if self.value == "*" or other.value == "*":
            return None

        # "/foo/{x}/baz" and "/foo/bar/{y}" will both match on "/foo/bar/baz".
        # We allow ambiguous paths if one is strictly more specific than the other,
        # but error on equivalent specificity.
        self_segments = self.value.strip("/").split("/")
        other_segments = other.value.strip("/").split("/")

        if len(self_segments) != len(other_segments):
            return None

        is_self_more_specific = False  # ...in at least one segment
        is_other_more_specific = False  # ...in at least one segment

        for self_seg, other_seg in zip(self_segments, other_segments):
            is_self_param = self._is_placeholder_segment(self_seg)
            is_other_param = self._is_placeholder_segment(other_seg)

            if not is_self_param and not is_other_param:
                if self_seg != other_seg:
                    return None
                continue

            if is_self_param and is_other_param:
                continue

            if is_self_param:
                is_other_more_specific = True
            else:
                is_self_more_specific = True

        if is_self_more_specific ^ is_other_more_specific:
            return None

        return f"URL patterns '{self.value}' and '{other.value}' match the same URLs"

    async def matches(self, req: web.Request) -> bool:
        if self.value == "*":
            return True

        return (
            DynamicResource(self.value)._match(  # pyright: ignore[reportPrivateUsage]
                req.path
            )
            is not None
        )

    @property
    def specificity_index(self) -> float:
        if self.value == "*":
            return float("-inf")
        segments = self.value.strip("/").split("/")
        return sum(1 for s in segments if not self._is_placeholder_segment(s))


class _KeyValueMatch(BaseModel):
    key: str
    value: str

    def list_compatibility_errors(self, other: CollectionMatchingSpec) -> str | None:
        if not isinstance(other, type(self)):
            return None
        if self.key != other.key:
            return None
        if self.value == "*" and other.value == "*":
            return (
                f"The '*' wildcard can only be used in one "
                f"{type(self).__name__}(key={self.key!r}) definition"
            )
        if self.value == other.value:
            return (
                f"Only one {type(self).__name__}(key={self.key!r}) "
                f"can claim the {self.value!r} discriminator value"
            )
        return None

    @property
    def specificity_index(self) -> float:
        return 0 if self.value == "*" else float("inf")


class HeaderMatch(_KeyValueMatch):
    type: Literal["header"] = "header"

    async def matches(self, req: web.Request) -> bool:
        header_value = req.headers.get(self.key)
        if header_value is None:
            return False
        return self.value == "*" or header_value == self.value


# TODO: Is the logic between concrete match instances and wildcards different enough that they warrant being separate classes?
# Do we actually want wildcards for bodies and headers if we'll be updating our configs with the discovered values later on?
class BodyMatch(_KeyValueMatch):
    type: Literal["body"] = "body"
    key: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

    async def matches(self, req: web.Request) -> bool:
        if self.value == "*":
            return True

        if "_parsed_body" not in req:
            # Cache the result to avoid re-parsing on every match rule
            req["_parsed_body"] = await req.json()

        body: dict[str, JsonValue] = req["_parsed_body"]

        for segment in self.key.split("."):
            value = body.get(segment)

            if value is None:
                return False

        return str(value) == self.value


CollectionMatchingSpec = Annotated[
    UrlMatch | HeaderMatch | BodyMatch,
    Field(discriminator="type"),
]


# ------------------------------------------------------------------------------
# Discriminator types
# ------------------------------------------------------------------------------


class Discriminator(BaseModel, metaclass=ABCMeta):
    known_values: set[str] = set()

    @abstractmethod
    def for_value(self, value: str): ...

    def create_match_rules(self):
        return [self.for_value(value) for value in self.known_values]


class UrlDiscriminator(BaseModel):
    type: Literal["url"] = "url"
    known_values: set[str] = set()

    @model_validator(mode="after")
    def _validate_known_values(self) -> UrlDiscriminator:
        for v in self.known_values:
            if v != "*":
                _ = DynamicResource(v)
        return self

    def for_value(self, value: str) -> UrlMatch:
        return UrlMatch(value=value)


class HeaderDiscriminator(BaseModel):
    type: Literal["header"] = "header"
    key: str
    known_values: set[str] = set()

    def for_value(self, value: str) -> HeaderMatch:
        return HeaderMatch(key=self.key, value=value)


class BodyDiscriminator(BaseModel):
    type: Literal["body"] = "body"
    dot_path: str  # dot-path into request body
    known_values: set[str] = set()

    def for_value(self, value: str) -> BodyMatch:
        return BodyMatch(key=self.dot_path, value=value)


CollectionDiscriminatorSpec = Annotated[
    UrlDiscriminator | HeaderDiscriminator | BodyDiscriminator,
    Field(discriminator="type"),
]
"""Essentially a factory for resource matching rules."""


CATCH_ALL_DISCRIMINATOR = UrlDiscriminator().for_value("*")
GENERIC_WEBHOOK_RESOURCE = Resource(
    name="webhook-data",
    key=["/_meta/webhookId"],
    model=WebhookDocument,
    open=_open_webhook_binding,
    initial_state=ResourceState(),
    initial_config=WebhookResourceConfig(
        name="webhook-data", match_rule=CATCH_ALL_DISCRIMINATOR
    ),
    schema_inference=True,
)


class WebhookCaptureSpec(BaseModel):
    name: str = Field(
        description="Unique name representing the set of captured messages"
    )
    discriminator: CollectionDiscriminatorSpec = Field(default_factory=UrlDiscriminator)

    def create_resources(
        self,
    ) -> list[Resource[_WebhookDocument, _ResourceConfig, _ResourceState]]:
        # TODO: Tighten this up so it only applies to bare bones instantiations
        if not self.discriminator.known_values:
            return [GENERIC_WEBHOOK_RESOURCE]

        return [
            Resource(
                name=f"{self.name}_{value}",
                key=["/_meta/webhookId"],
                model=WebhookDocument,
                open=_open_webhook_binding,
                initial_state=ResourceState(),
                initial_config=WebhookResourceConfig(
                    name=value,
                    match_rule=self.discriminator.for_value(value),
                ),
                schema_inference=True,
            )
            for value in self.discriminator.known_values
        ]


def list_spec_compatibility_errors(specs: list[WebhookCaptureSpec]) -> Generator[str]:
    """
    We do not want to assume all webhook messages originating from the same source will share
    the same format, which means we need to support multiple coexisting specs. This however,
    introduces the risk of routing conflicts. The simplest example would be two specs that
    accept all incoming messages to `/`, where it would be unclear to which collection docs
    should be written to.

    This function ensures there is no overlap between `WebhookCaptureSpec` definitions.
    """
    all_resources = [resource for spec in specs for resource in spec.create_resources()]
    all_match_rules = [resource.initial_config.match_rule for resource in all_resources]
    all_combinations = combinations(all_match_rules, 2)

    return filter(
        bool,
        [
            rule.list_compatibility_errors(other_rule)
            for rule, other_rule in all_combinations
        ],
    )


async def _run_webhook_server(
    specs: list[WebhookCaptureSpec],
    binding_index_mapping: Mapping[
        int, Resource[_WebhookDocument, _ResourceConfig, _ResourceState]
    ],
    task: Task,
):
    # TODO: We need to classify webhook processing features as pre- and post-
    # collection routing. So, we need an universal handler for all requests,
    # then it gets routed to the collection handler.

    # Sort resources by their match rule, going from most to least specific.
    # The ordering currently is:
    #     1. Concrete body and header matches
    #     2. URL paths, from most to least specific
    #     3. body and header wildcards
    #     4. URL wildcards
    binding_index_mapping = dict(
        sorted(
            binding_index_mapping.items(),
            key=lambda idx_and_rsc: idx_and_rsc[
                1
            ].initial_config.match_rule.specificity_index,
            reverse=True,
        )
    )
    _rejecting = False

    async def webhook_handler(req: web.Request) -> web.Response:
        if _rejecting:
            return web.Response(status=503, text="Server shutting down")

        matching_binding_index = next(
            (
                idx
                for idx, rsc in binding_index_mapping.items()
                if rsc.initial_config.match_rule.matches(req)
            ),
            None,
        )
        if matching_binding_index is None:
            # TODO: Log error message and exit
            return web.Response(status=500)

        task.captured(
            matching_binding_index,
            WebhookDocument.model_validate(await req.json()),
        )
        await task.checkpoint(state=ConnectorState())

        return web.Response(text="{published: 1}")

    listener = web.Application()
    _ = listener.router.add_post("/{path:.*}", webhook_handler)

    runner = web.AppRunner(listener)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()

    try:
        _ = await task.stopping.webhook_event.wait()
        task.log.debug("webhook server is yielding to stop")
        _rejecting = True
    finally:
        await runner.cleanup()


def start_webhook_server(
    specs: list[WebhookCaptureSpec],
    binding_index_mapping: Mapping[
        int, Resource[_WebhookDocument, _ResourceConfig, _ResourceState]
    ],
    task: Task,
):
    """Start the webhook listener for the given bindings.

    Called from common.open() after resolving webhook bindings.
    Runs outside the main TaskGroup so the webhook server survives
    until all non-webhook tasks complete their graceful shutdown.
    """
    task.log.info("Starting webhook server")

    # TODO: We want to reject bad messages as fast as possible.
    # 1. Verify IP ranges, signatures, auth, everything
    # 2. Only then, route to the appropriate collection handling fn
    async def run():
        async with asyncio.TaskGroup() as webhook_tg:
            try:
                webhook_task = Task(
                    task.log.getChild("webhook-server"),
                    task.connector_status,
                    "capture.webhook-server",
                    task._output,
                    task.stopping,
                    webhook_tg,
                )
                await _run_webhook_server(specs, binding_index_mapping, webhook_task)
            except Exception as exc:
                task.log.error("".join(traceback.format_exception(exc)))
                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.pull_api_event.set()

    task.stopping.webhook_task = asyncio.create_task(run())
