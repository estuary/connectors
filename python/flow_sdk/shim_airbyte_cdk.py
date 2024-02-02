import logging
import os
import typing as t
from airbyte_cdk.sources.source import Source
from airbyte_protocol.models import (
    SyncMode,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    Status,
    Level as LogLevel,
)
from pydantic import BaseModel

from .capture import Connector, request, response, Response
from .flow import CaptureBinding
from . import flow, ValidateError, logger
from .logger import init_logger

logger = init_logger()

# `logger` has name "flow", and we thread it through the Airbyte APIs,
# but connectors may still get their own "airbyte" logger.
# Patch it to use the same log level as the "flow" Logger.
logging.getLogger("airbyte").setLevel(logger.level)

DOCS_URL = os.getenv("DOCS_URL")

class StreamState(BaseModel):
    state: AirbyteStateMessage
    rowId: int
"""
Airbyte doesn't appear to reduce state like we do. As a result,
the Airbyte CDK is expecting its state input to look like a list of
AirbyteStateMessages. Since we _do_ reduce state, what we do here is
keep track of the latest AirbyteStateMessage for each stream, and then
just give Airbyte a list of all of the latest state messages on boot.
"""
class State(BaseModel):
    bindingStateV1: t.Dict[str, StreamState] = {}

    """
    Update the latest known state value for a particular stream,
    as identified by its state_key
    """
    def handle_message(self, msg: AirbyteStateMessage, state_key: str):
        if msg.stream == None:
            raise Exception(
                "Got a STREAM-specific state message with no stream-specific state"
            )
        if not state_key in self.bindingStateV1:
            self.bindingStateV1[state_key] = StreamState(state=msg, rowId=0)
        self.bindingStateV1[state_key].state = msg

    def to_airbyte_input(self):
        return [streamState.state for k, streamState in self.bindingStateV1.items()]

    """
    Generate the final state value emitted to the Flow runtime.
    We're mixing in the latest known rowIds for each binding.
    This will generate an object that looks like this:
    {
        "bindingStateV1": {
            "my/stateKey.v2": {
                "state": {
                    "airbyte": "black box"
                },
                "rowId": 451234
            }
        }
    }
    """    
    def to_flow_state(self, index: dict[t.Tuple[str | None, str], t.List[int]], bindings: t.List[CaptureBinding]):
        for binding in bindings:
            if binding["stateKey"] in self.bindingStateV1:
                namespace = binding["resourceConfig"].get("namespace",None)
                stream = binding["resourceConfig"]["stream"]
                rowId = index.get((namespace, stream), [0,1])[1]
                self.bindingStateV1[binding["stateKey"]].rowId = rowId

        return self.dict()

    """
    This takes a Flow state object that was generated by `to_flow_state`
    as well as a list of bindings, and loads it into a State instance,
    as well as calculating the current binding-rowId index. We need to
    re-generate the index here as it's possible that binding indices have changed
    """
    @staticmethod
    def from_flow_state(state: dict | None, bindings: t.List[CaptureBinding]):
        try:
            parsed_state = state and State(**state) or State()

            index: dict[t.Tuple[str | None, str], t.List[int]] = {}
            for i, binding in enumerate(bindings):
                if binding["stateKey"] in parsed_state.bindingStateV1:
                    namespace = binding["resourceConfig"].get("namespace",None)
                    stream = binding["resourceConfig"]["stream"]
                    index[(namespace, stream)] = [i, parsed_state.bindingStateV1[binding["stateKey"]].rowId]
            return (parsed_state, index)

        except Exception as e:
            logger.error("Failed to parse incoming state: ", e)
            raise e

class CaptureShim(Connector):
    delegate: Source
    usesSchemaInference: bool

    def __init__(
        self, delegate: Source, oauth2: flow.OAuth2 | None, usesSchemaInference=True
    ):
        super().__init__()
        self.delegate = delegate
        self.oauth2 = oauth2
        self.usesSchemaInference = usesSchemaInference

    def spec(self, _: request.Spec) -> flow.Spec:
        spec = self.delegate.spec(logger)

        out: flow.Spec = {
            "documentationUrl": f"{DOCS_URL if DOCS_URL else spec.documentationUrl}",
            "configSchema": spec.connectionSpecification,
            "resourceConfigSchema": resource_config_schema,
            "resourcePathPointers": ["/namespace", "/stream"],
        }

        # TODO(johnny): Can we map spec.advanced_auth into flow.OAuth2 ?
        if self.oauth2:
            out["oauth2"] = self.oauth2

        return out

    def discover(self, discover: request.Discover) -> response.Discovered:
        catalog = self.delegate.discover(logger, discover["config"])

        bindings: t.List[response.DiscoveredBinding] = []

        for stream in catalog.streams:
            if SyncMode.incremental in stream.supported_sync_modes:
                sync_mode = SyncMode.incremental
            elif SyncMode.full_refresh in stream.supported_sync_modes:
                sync_mode = SyncMode.full_refresh
            else:
                raise RuntimeError("invalid sync modes", stream.supported_sync_modes)

            config: StreamResourceConfig = {
                "stream": stream.name,
                "syncMode": sync_mode,
            }
            if stream.namespace:
                config["namespace"] = stream.namespace
            if stream.default_cursor_field:
                config["cursorField"] = stream.default_cursor_field

            if stream.source_defined_primary_key:
                # Map array of array of property names into an array of JSON pointers.
                key = [
                    "/"
                    + "/".join(
                        p.replace("~", "~0").replace("/", "~1") for p in component
                    )
                    for component in stream.source_defined_primary_key
                ]
            elif sync_mode == SyncMode.full_refresh:
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

            if self.usesSchemaInference:
                stream.json_schema["x-infer-schema"] = True

            bindings.append(
                response.DiscoveredBinding(
                    recommendedName=stream.name,
                    documentSchema=stream.json_schema,
                    resourceConfig=t.cast(dict, config),
                    key=key,
                )
            )

        return response.Discovered(bindings=bindings)

    def validate(self, validate: request.Validate) -> response.Validated:
        result = self.delegate.check(logger, validate["config"])
        if result.status != Status.SUCCEEDED:
            raise ValidateError(f"{result.message}")

        bindings: t.List[response.ValidatedBinding] = []

        for b in validate["bindings"]:
            config = t.cast(StreamResourceConfig, b["resourceConfig"])

            if ns := config.get("namespace"):
                resource_path = [ns, config["stream"]]
            else:
                resource_path = [config["stream"]]

            bindings.append(response.ValidatedBinding(resourcePath=resource_path))

        return response.Validated(bindings=bindings)

    def open(self, open: request.Open, emit: t.Callable[[Response], None]) -> None:
        streams: t.List[ConfiguredAirbyteStream] = []
        bindings = open["capture"]["bindings"]
        
        # Index Key: (namespace, stream).
        # Indx Value: [binding index, next row_id].
        state, index = State.from_flow_state(open["state"], bindings)

        for i, binding in enumerate(bindings):
            rc = t.cast(StreamResourceConfig, binding["resourceConfig"])

            index_key = (rc.get("namespace"), rc["stream"])
            if not index_key in index:
                index[index_key] = [i, 1]

            streams.append(
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        json_schema=binding["collection"]["writeSchema"],
                        name=rc["stream"],
                        namespace=rc.get("namespace"),
                        supported_sync_modes=[rc["syncMode"]],
                    ),
                    cursor_field=rc.get("cursorField"),
                    destination_sync_mode="append",
                    sync_mode=rc["syncMode"],
                )
            )

        catalog = ConfiguredAirbyteCatalog(streams=streams)
        config = open["capture"]["config"]

        emit(Response(opened=response.Opened(explicitAcknowledgements=False)))

        for message in self.delegate.read(
            logger, config, catalog, state.to_airbyte_input()
        ):
            if record := message.record:
                entry = index.get((record.namespace, record.stream), None)
                if entry is None:
                    logger.warn(f"Document read in unrecognized stream {record.stream} (namespace: {record.namespace})")
                    continue

                record.data.setdefault("_meta", {})["row_id"] = entry[1]
                entry[1] += 1

                emit(
                    Response(
                        captured=response.Captured(binding=entry[0], doc=record.data)
                    )
                )

            elif state_msg := message.state:
                state_msg = t.cast(AirbyteStateMessage, state_msg)

                if state_msg.type != AirbyteStateType.STREAM:
                    raise Exception(
                        f"Unsupported Airbyte state type {state_msg.type}"
                    ) 

                binding_lookup = index.get((state_msg.stream.stream_descriptor.namespace, state_msg.stream.stream_descriptor.name), None)
                if binding_lookup is None:
                    logger.warn(f"Received state message for unrecognized stream {state_msg.stream.stream_descriptor.name} (namespace: {state_msg.stream.stream_descriptor.namespace})")                
                    continue
                
                # index values: [binding_index, row_id]
                stream_binding: CaptureBinding = bindings[binding_lookup[0]]

                state.handle_message(state_msg, stream_binding["stateKey"])
                emit(
                    Response(
                        checkpoint=response.Checkpoint(
                            state=flow.ConnectorState(
                                updated=state.to_flow_state(index, bindings), mergePatch=False
                            )
                        )
                    )
                )

            elif trace := message.trace:
                if error := trace.error:
                    logger.error(
                        error.message,
                        extra={
                            "internal_message": error.internal_message,
                            "stack_trace": error.stack_trace,
                        },
                    )
                elif estimate := trace.estimate:
                    logger.info(
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
                    logger.info(
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
                if log.level == LogLevel.DEBUG:
                    level = logging.DEBUG
                elif log.level == LogLevel.INFO:
                    level = logging.INFO
                elif log.level == LogLevel.WARN:
                    level = logging.WARNING
                else:
                    level = logging.ERROR

                logger.log(level, log.message, log.stack_trace)

            else:
                raise RuntimeError("unexpected AirbyteMessage", message)
        # Emit a final state message
        emit(
            Response(
                checkpoint=response.Checkpoint(
                    state=flow.ConnectorState(updated=state.to_flow_state(index, bindings), mergePatch=False)
                )
            )
        )


class StreamResourceConfig(t.TypedDict):
    stream: str
    syncMode: SyncMode
    namespace: t.NotRequired[str]
    cursorField: t.NotRequired[t.List[str]]


resource_config_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "stream": {"type": "string"},
        "syncMode": {"type": "string", "enum": ["incremental", "full_refresh"]},
        "namespace": {"type": ["string", "null"]},
        "cursorField": {
            "type": ["array", "null"],
            "items": {"type": "string"},
        },
    },
    "required": ["stream", "syncMode"],
    "title": "ResourceSpec"
}
