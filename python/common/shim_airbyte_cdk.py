import logging
import typing as t
from airbyte_cdk.sources.source import Source
from airbyte_protocol.models import (
    SyncMode,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    AirbyteStream,
    Status,
    Level as LogLevel,
)

from .capture import Connector, request, response, Response
from . import flow, logger, ValidateError

# `logger` has name "flow", and we thread it through the Airbyte APIs,
# but connectors may still get their own "airbyte" logger.
# Patch it to use the same log level as the "flow" Logger.
logging.getLogger("airbyte").setLevel(logger.level)


class CaptureShim(Connector):
    delegate: Source

    def __init__(
        self,
        delegate: Source,
        oauth2: flow.OAuth2 | None,
    ):
        super().__init__()
        self.delegate = delegate
        self.oauth2 = oauth2

    def spec(self, _: request.Spec) -> flow.Spec:
        spec = self.delegate.spec(logger)

        out: flow.Spec = {
            "documentationUrl": f"{spec.documentationUrl}",
            "configSchema": spec.connectionSpecification,
            "resourceConfigSchema": resource_config_schema,
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
            else:
                raise RuntimeError(
                    "incremental stream is missing a source-defined primary key",
                    stream.name,
                )

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

        emit(Response(opened=response.Opened(explicitAcknowledgements=False)))

        for message in self.delegate.read(logger, config, catalog):
            if record := message.record:
                entry = index[(record.namespace, record.stream)]
                record.data.setdefault("_meta", {})["row_id"] = entry[1]
                entry[1] += 1

                emit(Response(
                    captured=response.Captured(binding=entry[0], doc=record.data)
                ))

            elif state := message.state:
                # TODO(johnny): handle various state types, and mix in next_row_id.
                raise NotImplemented

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


class StreamResourceConfig(t.TypedDict):
    stream: str
    syncMode: SyncMode
    namespace: t.NotRequired[str]
    cursorField: t.NotRequired[t.List[str]]


resource_config_schema = {
    "type": "object",
    "properties": {
        "stream": {"type": "string"},
        "syncMode": {"enum": ["incremental", "full_refresh"]},
        "namespace": {"type": "string"},
        "cursorField": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["stream", "syncMode"],
}
