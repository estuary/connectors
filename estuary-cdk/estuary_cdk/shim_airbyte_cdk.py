from dataclasses import dataclass
from logging import Logger
from pydantic import Field
from typing import Any, ClassVar, Annotated, Callable, Awaitable, List, Literal
import logging
import os


from .capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from .flow import CaptureBinding, OAuth2Spec, ConnectorSpec
from . import ValidationError

from airbyte_cdk.sources.source import Source as AirbyteSource
from airbyte_protocol.models import (
    SyncMode as AirbyteSyncMode,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    Status as AirbyteStatus,
    Level as AirbyteLevel,
)


# `logger` has name "flow", and we thread it through the Airbyte APIs,
# but connectors may still get their own "airbyte" logger.
# Patch it to use the same log level as the "flow" Logger.
# logging.getLogger("airbyte").setLevel(logger.level)

DOCS_URL = os.getenv("DOCS_URL")


EndpointConfig = dict
"""This shim imposes no bound on EndpointConfig"""


class ResourceConfig(common.BaseResourceConfig, extra="forbid"):
    """ResourceConfig encodes a configured resource stream"""

    PATH_POINTERS: ClassVar[list[str]] = ["/namespace", "/stream"]

    stream: Annotated[str, Field(description="Name of this stream")]

    sync_mode: Literal["full_refresh", "incremental"] = Field(
        alias="syncMode",
        title="Sync Mode",
        description="Sync this resource incrementally, or fully refresh it every run",
    )

    namespace: Annotated[
        str | None, Field(description="Enclosing schema namespace of this resource")
    ] = None

    cursor_field: list[str] | None = Field(
        alias="cursorField", title="Cursor Field", default=None
    )

    def path(self) -> list[str]:
        if self.namespace:
            return [self.namespace, self.stream]

        return [self.stream]


class ResourceState(common.BaseResourceState, extra="forbid"):
    """ResourceState wraps an AirbyteStateMessage"""

    rowId: int
    state: dict[str, Any] = {}
    """state is a dict encoding of AirbyteStateMessage"""


class ConnectorState(common.ConnectorState[ResourceState], extra="ignore"):
    """ConnectorState represents a number of ResourceStates, keyed by binding state key.
    
    Top-level fields other than bindingStateV1 are ignored, to allow for a lossy migration from
    states that existed prior to adopting this convection. Connectors transitioning in this way will
    effectively start over from the beginning.
    """

    bindingStateV1: dict[str, ResourceState] = {}


class Document(common.BaseDocument, extra="allow"):
    pass


def escape_field(field: str) -> str:
    return field.replace("~", "~0").replace("/", "~1")


def transform_airbyte_key(key: str | List[str] | List[List[str]]) -> List[str]:
    key_fields: List[str] = []

    if isinstance(key, str):
        # key = "piz/za"
        # key_fields: ["piz~1za"]
        key_fields = [escape_field(key)]
    elif isinstance(key, list):
        for component in key:
            if isinstance(component, str):
                # key = ["piz/za", "par~ty"]
                # key_fields: ["piz~1za", "pa~0rty"]
                key_fields.append(escape_field(component))
            elif isinstance(component, list):
                # key = [["pizza", "che/ese"], "potato"]
                # Implies a document like {"potato": 12, "pizza": {"che/ese": 5}}
                # key_fields: ["pizza/che~1ese", "potato"]
                key_fields.append(
                    "/".join((escape_field(field) for field in component))
                )
            else:
                raise ValueError(f"Invalid key component: {component}")

    return key_fields


@dataclass
class CaptureShim(BaseCaptureConnector):
    delegate: AirbyteSource
    schema_inference: bool
    oauth2: OAuth2Spec | None

    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def _all_resources(
        self, log: Logger, config: EndpointConfig
    ) -> list[common.Resource[Document, ResourceConfig, ResourceState]]:
        logging.getLogger("airbyte").setLevel(log.level)
        catalog = self.delegate.discover(log, config)

        resources: list[common.Resource[Any, ResourceConfig, ResourceState]] = []

        for stream in catalog.streams:

            resource_config = ResourceConfig(
                stream=stream.name, syncMode="full_refresh"
            )

            if AirbyteSyncMode.incremental in stream.supported_sync_modes:
                resource_config.sync_mode = "incremental"
            elif AirbyteSyncMode.full_refresh in stream.supported_sync_modes:
                pass
            else:
                raise RuntimeError("invalid sync modes", stream.supported_sync_modes)

            if stream.namespace:
                resource_config.namespace = stream.namespace
            if stream.default_cursor_field:
                resource_config.cursor_field = stream.default_cursor_field

            if stream.source_defined_primary_key:
                # Map array of array of property names into an array of JSON pointers.
                key = [
                    "/" + key
                    for key in transform_airbyte_key(stream.source_defined_primary_key)
                ]
            elif resource_config.sync_mode == "full_refresh":
                # Synthesize a key based on the record's order within each stream refresh.
                key = ["/_meta/row_id"]
            else:
                raise RuntimeError(
                    "incremental stream is missing a source-defined primary key",
                    stream.name,
                )

            # Extend schema with /_meta/row_id, since we always generate it
            meta = stream.json_schema.setdefault("properties", {}).setdefault(
                "_meta", {"type": "object"}
            )
            meta.setdefault("properties", {})["row_id"] = {"type": "integer"}
            meta.setdefault("required", []).append("row_id")

            # if self.schema_inference:
            #    stream.json_schema["x-infer-schema"] = True

            resources.append(
                common.Resource(
                    name=stream.name,
                    key=key,
                    model=common.Resource.FixedSchema(stream.json_schema),
                    open=lambda binding, index, state, task: None,  # No-op.
                    initial_state=ResourceState(rowId=0),
                    initial_config=resource_config,
                    schema_inference=self.schema_inference,
                )
            )

        return resources

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        logging.getLogger("airbyte").setLevel(log.level)
        spec = self.delegate.spec(log)

        return ConnectorSpec(
            configSchema=spec.connectionSpecification,
            documentationUrl=f"{DOCS_URL if DOCS_URL else spec.documentationUrl}",
            oauth2=self.oauth2,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self,
        log: Logger,
        discover: request.Discover[EndpointConfig],
    ) -> response.Discovered:
        resources = await self._all_resources(log, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:

        result = self.delegate.check(log, validate.config)
        if result.status != AirbyteStatus.SUCCEEDED:
            raise ValidationError([f"{result.message}"])

        resources = await self._all_resources(log, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)

        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:

        resources = await self._all_resources(log, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)

        async def _run(task: Task) -> None:
            await self._run(task, resolved, open.capture.config, open.state)

        return (response.Opened(explicitAcknowledgements=False), _run)

    async def _run(
        self,
        task: Task,
        resolved: list[
            tuple[
                CaptureBinding[ResourceConfig],
                common.Resource[Document, ResourceConfig, ResourceState],
            ]
        ],
        config: EndpointConfig,
        connector_state: ConnectorState,
    ) -> None:

        airbyte_streams: list[ConfiguredAirbyteStream] = [
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    json_schema=binding.collection.writeSchema,
                    name=binding.resourceConfig.stream,
                    namespace=binding.resourceConfig.namespace,
                    supported_sync_modes=[binding.resourceConfig.sync_mode],
                ),
                cursor_field=binding.resourceConfig.cursor_field,
                destination_sync_mode="append",
                sync_mode=binding.resourceConfig.sync_mode,
            )
            for binding, resource in resolved
        ]
        airbyte_catalog = ConfiguredAirbyteCatalog(streams=airbyte_streams)

        if "bindingStateV1" not in connector_state.__fields_set__:
            # Initialize the top-level state object so that it is properly serialized if this is an
            # "empty" state, which occurs for a brand new task that has never emitted any
            # checkpoints.
            connector_state.__setattr__("bindingStateV1", {})

        # Index of Airbyte (namespace, stream) => ResourceState.
        # Use `setdefault()` to initialize ResourceState if it's not already part of `connector_state`.
        index: dict[tuple[str | None, str], tuple[int, ResourceState]] = {
            (
                binding.resourceConfig.namespace,
                binding.resourceConfig.stream,
            ): (
                index,
                connector_state.bindingStateV1.setdefault(
                    binding.stateKey, resource.initial_state
                ),
            )
            for index, (binding, resource) in enumerate(resolved)
        }

        airbyte_states: list[AirbyteStateMessage] = [
            AirbyteStateMessage(**rs.state) for _, rs in index.values() if rs.state
        ]

        for message in self.delegate.read(
            task.log, config, airbyte_catalog, airbyte_states
        ):
            if record := message.record:
                entry = index.get((record.namespace, record.stream), None)
                if entry is None:
                    task.log.warn(
                        f"Document read in unrecognized stream {record.stream} (namespace: {record.namespace})"
                    )
                    continue

                doc = Document(
                    meta_=Document.Meta(op="u", row_id=entry[1].rowId), **record.data
                )
                entry[1].rowId += 1

                task.captured(entry[0], doc)

            elif state_msg := message.state:

                if state_msg.type != AirbyteStateType.STREAM:
                    raise RuntimeError(
                        f"Unsupported Airbyte state type {state_msg.type}"
                    )
                elif state_msg.stream is None:
                    raise RuntimeError(
                        "Got a STREAM-specific state message with no stream-specific state"
                    )

                entry = index.get(
                    (
                        state_msg.stream.stream_descriptor.namespace,
                        state_msg.stream.stream_descriptor.name,
                    ),
                    None,
                )
                if entry is None:
                    task.log.warn(
                        f"Received state message for unrecognized stream {state_msg.stream.stream_descriptor.name} (namespace: {state_msg.stream.stream_descriptor.namespace})"
                    )
                    continue

                entry[1].state = state_msg.dict()

                task.checkpoint(connector_state, merge_patch=False)

            elif trace := message.trace:
                if error := trace.error:
                    task.log.error(
                        error.message,
                        extra={
                            "internal_message": error.internal_message,
                            "stack_trace": error.stack_trace,
                        },
                    )
                elif estimate := trace.estimate:
                    task.log.info(
                        "progress estimate",
                        extra={
                            "estimate": {
                                "stream": estimate.name,
                                "namespace": estimate.namespace,
                                "type": estimate.type,
                                "row_estimate": estimate.row_estimate,
                                "byte_estimate": estimate.byte_estimate,
                            }
                        },
                    )
                elif status := trace.stream_status:
                    task.log.info(
                        "stream status",
                        extra={
                            "status": {
                                "stream": status.stream_descriptor.name,
                                "namespace": status.stream_descriptor.namespace,
                                "status": status.status,
                            }
                        },
                    )
                else:
                    raise RuntimeError(f"unexpected trace type {trace.type}")

            elif log := message.log:
                if log.level == AirbyteLevel.DEBUG:
                    level = logging.DEBUG
                elif log.level == AirbyteLevel.INFO:
                    level = logging.INFO
                elif log.level == AirbyteLevel.WARN:
                    level = logging.WARNING
                else:
                    level = logging.ERROR

                task.log.log(level, log.message, log.stack_trace)

            else:
                raise RuntimeError("unexpected AirbyteMessage", message)

        # Emit a final checkpoint before exiting.
        task.checkpoint(connector_state, merge_patch=False)
        return None
