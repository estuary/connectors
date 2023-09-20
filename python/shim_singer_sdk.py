import os
import singer_sdk._singerlib as singer
import singer_sdk.metrics
import typing as t

from .capture import Connector, request, response, Response
from . import flow, logger

class Config(t.TypedDict):
    pass


class State(t.TypedDict):
    pass


def write_message_panics(message: singer.Message) -> None:
    raise RuntimeError("unexpected call to singer.write_message", message.to_dict())


# Disable the _setup_logging routine, as it otherwise clobbers our logging setup.
singer_sdk.metrics._setup_logging = lambda _config: None
# The singer-sdk uses LOGLEVEL to set up loggers instead of LOG_LEVEL.
os.environ.setdefault("LOGLEVEL", os.environ.get("LOG_LEVEL", "INFO").upper())
# CaptureShim instruments singer.write_message as-needed to capture message callbacks.
# We don't expect it to ever be called outside of those specific contexts.
singer.write_message = write_message_panics


class CaptureShim(Connector):
    config_schema: dict
    delegate_factory: t.Callable[[Config, singer.Catalog | None, State | None], singer_sdk.Tap]

    def __init__(
        self,
        config_schema: dict,
        delegate_factory: t.Callable[
            [Config, singer.Catalog | None, State | None], singer_sdk.Tap
        ],
    ):
        super().__init__()
        self.config_schema = config_schema
        self.delegate_factory = delegate_factory

    def spec(self, _: request.Spec) -> flow.Spec:
        out = flow.Spec(
            documentationUrl="not://sure",  # TODO
            configSchema=self.config_schema,
            resourceConfigSchema=resource_config_schema,
        )

        # TODO(johnny): Can we map spec.advanced_auth into flow.OAuth2 ?
        # jif self.oauth2:
        #    out["oauth2"] = self.oauth2

        return out

    def discover(self, discover: request.Discover) -> response.Discovered:
        config = t.cast(Config, discover["config"])
        delegate = self.delegate_factory(config, None, None)

        bindings: t.List[response.DiscoveredBinding] = []

        # Use stack-based iteration to enumerate all child streams.
        stack = [s for s in delegate.discover_streams()]

        while stack:
            stream: singer_sdk.Stream = stack.pop(0)
            stack.extend(stream.child_streams)

            entry: singer.CatalogEntry = stream._singer_catalog_entry

            # According to the Singer spec, key_properties are top-level
            # properties of the document. We extend this interpretation to
            # also allow for arbitrary JSON pointers.
            if entry.key_properties:
                key = [
                    p
                    if p.startswith("/")
                    else "/" + p.replace("~", "~0").replace("/", "~1")
                    for p in entry.key_properties
                ]
            else:
                key = ["/_meta/row_id"]

            for bc, meta in entry.metadata.items():
                if meta.inclusion == "available":
                    meta.selected = True

            resourceConfig = entry.to_dict()
            json_schema = resourceConfig.pop("schema")

            bindings.append(
                response.DiscoveredBinding(
                    recommendedName=entry.tap_stream_id,
                    documentSchema=json_schema,
                    resourceConfig=resourceConfig,
                    key=key,
                )
            )

        return response.Discovered(bindings=bindings)

    def validate(self, validate: request.Validate) -> response.Validated:
        catalog = singer.Catalog()
        config = t.cast(Config, validate["config"])
        bindings: t.List[response.ValidatedBinding] = []

        for binding in validate["bindings"]:
            entry = singer.CatalogEntry.from_dict(binding["resourceConfig"])
            entry.schema = singer.Schema.from_dict(binding["collection"]["writeSchema"])
            catalog[entry.tap_stream_id] = entry

            bindings.append(
                response.ValidatedBinding(resourcePath=[entry.tap_stream_id])
            )

        delegate = self.delegate_factory(config, catalog, None)

        def _on_message(message: singer.Message) -> None:
            logger.debug("connection test message", message)

        singer.write_message = _on_message
        _ = delegate.run_connection_test()  # Either returns True, or panics.
        singer.write_message = write_message_panics

        return response.Validated(bindings=bindings)

    def open(self, open: request.Open, emit: t.Callable[[Response], None]) -> None:
        catalog = singer.Catalog()
        config = t.cast(Config, open["capture"]["config"])
        state = t.cast(State, open["state"])

        # Key: tap_stream_id.
        # Value: [binding index, next row_id].
        index: dict[str, t.List[int]] = {}

        for i, binding in enumerate(open["capture"]["bindings"]):
            entry = singer.CatalogEntry.from_dict(binding["resourceConfig"])
            entry.schema = singer.Schema.from_dict(binding["collection"]["writeSchema"])

            # TODO(johnny): Re-hydrate next row_id from open["state"].
            index[entry.tap_stream_id] = [i, 1]
            catalog[entry.tap_stream_id] = entry

        delegate: singer_sdk.Tap = self.delegate_factory(config, catalog, state)
        emit(Response(opened=response.Opened(explicitAcknowledgements=False)))

        def _on_message(message: singer.Message):
            if message.type == singer.SingerMessageType.SCHEMA:
                return  # Ignored.
            elif message.type == singer.SingerMessageType.RECORD:
                record = t.cast(singer.RecordMessage, message)
                entry = index[record.stream]

                record.record.setdefault("_meta", {})["row_id"] = entry[1]
                entry[1] += 1

                emit(
                    Response(
                        captured=response.Captured(binding=entry[0], doc=record.record)
                    )
                )
            elif message.type == singer.SingerMessageType.STATE:
                state = t.cast(singer.StateMessage, message)
                logger.debug("stream state", state)
            else:
                raise RuntimeError("unexpected singer_sdk Message", message)

        singer.write_message = _on_message
        delegate.sync_all()
        singer.write_message = write_message_panics


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
