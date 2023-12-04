import logging
import os
import typing as t
from airbyte_cdk.sources.source import Source
from dataclasses import asdict, dataclass
from airbyte_protocol.models import (
    SyncMode,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    AirbyteStateMessage,
    AirbyteStreamState,
    AirbyteGlobalState,
    AirbyteStateType,
    AirbyteStream,
    Status,
    Level as LogLevel,
)

from .capture import Connector, request, response, Response
from . import flow, ValidateError, logger
from .logger import init_logger

logger = init_logger()

# `logger` has name "flow", and we thread it through the Airbyte APIs,
# but connectors may still get their own "airbyte" logger.
# Patch it to use the same log level as the "flow" Logger.
logging.getLogger("airbyte").setLevel(logger.level)

DOCS_URL = os.getenv("DOCS_URL")


"""
Airbyte doesn't appear to reduce state like we do. As a result,
the Airbyte CDK is expecting its state input to look like a list of
AirbyteStateMessages. Since we _do_ reduce state, what we do here is
keep track of the latest AirbyteStateMessage for each stream, and then
just give Airbyte a list of all of the latest state messages on boot.
"""
@dataclass
class State:
    stream_state: t.Optional[t.Dict[str, AirbyteStateMessage]] = None
    global_state: t.Optional[AirbyteStateMessage] = None

    def handle_message(self, msg: AirbyteStateMessage):
        if msg.type == AirbyteStateType.STREAM:
            if msg.stream == None:
                raise Exception(
                    "Got a STREAM-specific state message with no stream-specific state"
                )
            if not self.stream_state:
                self.stream_state = {}
            self.stream_state[
                f"{msg.stream.stream_descriptor.name}-{msg.stream.stream_descriptor.namespace}"
            ] = msg
        elif msg.type == AirbyteStateType.GLOBAL:
            self.global_state = msg
        elif msg.type == AirbyteStateType.LEGACY or msg.type is None:
            raise Exception("Airbyte LEGACY state is not supported")

    def to_airbyte_input(self):
        if self.global_state:
            return [self.global_state]
        elif self.stream_state:
            return [msg for k, msg in self.stream_state.items()]


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

            json_schema = stream.json_schema

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

                # Extend schema with /_meta/row_id.
                meta = stream.json_schema.setdefault("properties", {}).setdefault(
                    "_meta", {"type": "object"}
                )
                meta.setdefault("properties", {})["row_id"] = {"type": "integer"}
                meta.setdefault("required", []).append("row_id")
                json_schema = stream.json_schema
            else:
                raise RuntimeError(
                    "incremental stream is missing a source-defined primary key",
                    stream.name,
                )

            if self.usesSchemaInference:
                json_schema["x-infer-schema"] = True

            bindings.append(
                response.DiscoveredBinding(
                    recommendedName=stream.name,
                    documentSchema=json_schema,
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

        # Key: (namespace, stream).
        # Value: [binding index, next row_id].
        index: dict[t.Tuple[str | None, str], t.List[int]] = {}

        for i, binding in enumerate(open["capture"]["bindings"]):
            rc = t.cast(StreamResourceConfig, binding["resourceConfig"])

            # TODO(johnny): Re-hydrate next row_id from open["state"].
            index[(rc.get("namespace"), rc["stream"])] = [i, 1]

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
        state = State(**(open["state"] or {}))

        emit(Response(opened=response.Opened(explicitAcknowledgements=False)))

        for message in self.delegate.read(
            logger, config, catalog, state.to_airbyte_input()
        ):
            if record := message.record:
                entry = index[(record.namespace, record.stream)]
                record.data.setdefault("_meta", {})["row_id"] = entry[1]
                entry[1] += 1

                emit(
                    Response(
                        captured=response.Captured(binding=entry[0], doc=record.data)
                    )
                )

            elif state_msg := message.state:
                state_msg = t.cast(AirbyteStateMessage, state_msg)
                logger.info(f"Got a state message: {str(state_msg)}")
                # TODO(johnny): mix in next_row_id
                state.handle_message(state_msg)
                emit(
                    Response(
                        checkpoint=response.Checkpoint(
                            state=flow.ConnectorState(
                                updated=asdict(state), mergePatch=False
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
                    state=flow.ConnectorState(updated=asdict(state), mergePatch=False)
                )
            )
        )


class StreamResourceConfig(t.TypedDict):
    stream: str
    syncMode: SyncMode
    namespace: t.NotRequired[str]
    cursorField: t.NotRequired[t.List[str]]


resource_config_schema = {
    "type": "object",
    "properties": {
        "stream": {"type": "string"},
        "syncMode": {"type": "string", "enum": ["incremental", "full_refresh"]},
        "namespace": {"type": "string"},
        "cursorField": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["stream", "syncMode"],
}
