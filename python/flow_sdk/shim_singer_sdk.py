import os
import sys
import singer_sdk._singerlib as singer
import singer_sdk.metrics
from singer_sdk.streams import Stream
import typing as t

from .capture import Connector, request, response, Response
from . import flow, ValidateError, init_logger

logger = init_logger()

def write_message_panics(message: singer.Message) -> None:
    raise RuntimeError("unexpected call to singer.write_message", message.to_dict())

# Disable the _setup_logging routine, as it otherwise clobbers our logging setup.
singer_sdk.metrics._setup_logging = lambda _config: None
# The singer-sdk uses LOGLEVEL to set up loggers instead of LOG_LEVEL.
os.environ.setdefault("LOGLEVEL", os.environ.get("LOG_LEVEL", "INFO").upper())
# CaptureShim instruments singer.write_message as-needed to capture message callbacks.
# We don't expect it to ever be called outside of those specific contexts.
singer.write_message = write_message_panics

DOCS_URL = os.getenv("DOCS_URL")

class Config(t.TypedDict):
    pass

class State(t.TypedDict):
    pass


class DelegateFactoryCallable(t.Protocol):
    def __call__(self, config: Config, catalog: singer.Catalog | None = None, state: State | None = None) -> singer_sdk.Tap:
        ...

class CaptureShim(Connector):
    config_schema: dict
    delegate_factory: DelegateFactoryCallable
    usesSchemaInference: bool
    oauth2: flow.OAuth2 | None

    def __init__(
        self,
        config_schema: dict,
        delegate_factory: DelegateFactoryCallable,
        usesSchemaInference = True,
        oauth2: flow.OAuth2 | None = None
    ):
        super().__init__()
        self.config_schema = config_schema
        self.delegate_factory = delegate_factory
        self.usesSchemaInference = usesSchemaInference
        self.oauth2 = oauth2

    def spec(self, _: request.Spec) -> flow.Spec:
        out = flow.Spec(
            documentationUrl=DOCS_URL if DOCS_URL else "https://docs.estuary.dev",
            configSchema=self.config_schema,
            resourceConfigSchema=resource_config_schema,
            resourcePathPointers=["/stream"],
        )

        if self.oauth2:
           out["oauth2"] = self.oauth2

        return out

    def discover(self, discover: request.Discover) -> response.Discovered:
        config = t.cast(Config, discover["config"])
        delegate = self.delegate_factory(config=config)

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
            
            if self.usesSchemaInference:
                json_schema["x-infer-schema"] = True

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

            # It looks[1] like it's incorrect to "upgrade" a stream that does not
            # have a `replication_key` to use `INCREMENTAL` mode, but it should
            # be possible to "downgrade" an INCREMENTAL stream to FULL_TABLE
            # [1]: https://github.com/meltano/sdk/blob/main/singer_sdk/streams/core.py#L751-L756

            if entry.replication_method == "INCREMENTAL" and entry.replication_key is None:
                raise ValidateError(f"{entry.stream} does not support {entry.replication_method} replication.")

            bindings.append(
                response.ValidatedBinding(resourcePath=[entry.tap_stream_id])
            )

        delegate = self.delegate_factory(config=config, catalog=catalog)

        def _on_message(message: singer.Message) -> None:
            logger.debug("connection test message", message)

        singer.write_message = _on_message
        # run_connection_test() attempts to read from streams that are not selected
        # which we don't want -- we only want to validate that streams we care about work,
        # not that all possible streams work.
        _ = delegate.run_sync_dry_run(
            dry_run_record_limit=1,
            streams=[stream for stream in delegate.streams.values() if stream.selected],
        )
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

        delegate: singer_sdk.Tap = self.delegate_factory(config=config, catalog=catalog, state=state)
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
                # The Meltano SDK manages any stream-specific state for us.
                # See their docs on this here: https://sdk.meltano.com/en/latest/implementation/state.html
                emit(
                    Response(
                        checkpoint=response.Checkpoint(
                            state=flow.ConnectorState(
                                updated=state.to_dict(),
                                mergePatch=False
                            )
                        )
                    )
                )
            else:
                raise RuntimeError("unexpected singer_sdk Message", message)

        singer.write_message = _on_message
        self.sync_streams(delegate)
        singer.write_message = write_message_panics

    # Lifted from singer-sdk for the moment, but pulling this in-tree will
    # allow us to do fancy things like driving streams in parallel
    # instead of the current implementation which drives them sequentially.
    def sync_streams(self, delegate: singer_sdk.Tap): 
        delegate._reset_state_progress_markers()
        delegate._set_compatible_replication_methods()
        singer.write_message(singer.StateMessage(value=delegate.state))

        stream: Stream
        for stream in delegate.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                delegate.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                delegate.logger.debug(
                    "Child stream '%s' is expected to be called "
                    "by parent stream '%s'. "
                    "Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            delegate.logger.info("Syncing stream '%s'.", stream.name)
            stream.sync()
            # This is where we tell the Meltano SDK to do any final state emits if neccesary
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in delegate.streams.values():
            stream.log_sync_costs()


resource_config_schema = {
    "type": "object",
    "properties": {
        "stream": {"type": "string"},
        "replication_method": {"type": "string", "enum": ["INCREMENTAL", "FULL_TABLE"]},
        "replication_key": {"type": "string"}
    },
    "required": ["stream", "replication_method"]
}
