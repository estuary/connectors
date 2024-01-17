import os
import sys
import singer_sdk._singerlib as singer
import singer_sdk.metrics
from singer_sdk.streams import Stream
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
)
import typing as t

from .capture import Connector, request, response, Response
from .flow import CaptureBinding
from . import flow, ValidateError, init_logger

logger = init_logger()

def write_message_panics(message: singer.Message) -> None:
    raise RuntimeError("unexpected call to singer.write_message", message.to_dict())

class State(t.TypedDict):
    bindingStateV1: dict[str, t.Any]
    rowIds: dict[str, int] 

RESOURCE_PATH_POINTER = "tap_stream_id"
# To avoid collisions inside `bindingStateV1`
ESTUARY_NAMESPACE = "estuary.dev"
ROW_COUNTER_STATE_KEY = f"{ESTUARY_NAMESPACE}/rowCounter"

"""
The Meltano SDK is expecting bookmark keys to be `tap_stream_id`s. We use 
`tap_stream_id` as the _basis_ for our state keys, but the state key for a binding
can change over time, for example if that binding is re-backfilled. As such, we
need to translate between `tap_stream_id`-keyed state objects for Meltano, and
`stateKey`-keyed state objects for Flow.
"""
def singer_to_flow_state(state: singer.StateMessage, bindings: t.List[CaptureBinding], rowIds: dict[str, int]):
    flow_state = {}
    singer_state = state.value.get("bookmarks", {})
    for binding in bindings:
        try:
            # Singer keys states on the `tap_stream_id`
            singer_state_key = binding["resourceConfig"][RESOURCE_PATH_POINTER]
            # But we want the keys inside of `bindingStateV1` to be whatever
            # the binding's `state_key` is, so that the state key can be changed
            # for example in the case of an update to the backfill counter
            flow_state_key = binding["stateKey"]

            state_val = singer_state.get(singer_state_key)
            if state_val is not None:
                # Let's stuff the row counter into the state for this binding
                # so that it also gets blown away correctly when we change state keys
                state_val[ROW_COUNTER_STATE_KEY] = rowIds.get(singer_state_key)
                flow_state[flow_state_key] = state_val
        except Exception as e:
            logger.error(f'Error handling STATE message for binding {binding["stateKey"]}')
            raise e
    return State(**{"bindingStateV1": flow_state})

"""
Perform the opposite of the translation that `singer_to_flow_state` does,
turning `stateKey`-keyed state objects into `tap_stream_id`-keyed ones.
Also calculate the correct binding-rowId index given the current binding list.
"""
def flow_to_singer_state(state: State, bindings: t.List[CaptureBinding]):
    singer_state = {}
    rowIds: dict[str, t.List[int]] = {}
    flow_state = state.get("bindingStateV1", {})

    for i, binding in enumerate(bindings):
        try:
            singer_state_key = binding["resourceConfig"][RESOURCE_PATH_POINTER]
            flow_state_key = binding["stateKey"]

            state_val = flow_state.get(flow_state_key)
            if state_val is not None:
                singer_state[singer_state_key] = state_val
                rowIds[singer_state_key] = [i, state_val[ROW_COUNTER_STATE_KEY]]
        except Exception as e:
            logger.error(f'Error handling STATE message for binding {binding["stateKey"]}')
            raise e
    return (
        {"bookmarks": singer_state},
        rowIds
    )
# Disable the _setup_logging routine, as it otherwise clobbers our logging setup.
singer_sdk.metrics._setup_logging = lambda config: None
# The singer-sdk uses LOGLEVEL to set up loggers instead of LOG_LEVEL.
os.environ.setdefault("LOGLEVEL", os.environ.get("LOG_LEVEL", "INFO").upper())
# CaptureShim instruments singer.write_message as-needed to capture message callbacks.
# We don't expect it to ever be called outside of those specific contexts.
singer.write_message = write_message_panics

DOCS_URL = os.getenv("DOCS_URL")

# Replication modes from singer_sdk/streams/core.py
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"

class Config(t.TypedDict):
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
            resourcePathPointers=[f"/{RESOURCE_PATH_POINTER}"],
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

            if entry.replication_method == REPLICATION_INCREMENTAL and not entry.replication_key:
                raise RuntimeError(f"Stream {stream.name} must have a replication key in order to use {REPLICATION_INCREMENTAL} replication.")
            
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
                logger.info(f"Stream {stream.name} does not have a primary key, using '/_meta/row_id' as the key.")                 
                key = ["/_meta/row_id"]

            # Always include `_meta/row_id` in the schema, since we always set it
            row_id_type = Property("row_id", IntegerType, required=True)

            if entry.schema.properties is not None:
                if not "_meta" in entry.schema.properties:
                    entry.schema.properties["_meta"] = ObjectType(row_id_type)
                    if not entry.schema.required:
                        entry.schema.required = []
                    if not "_meta" in entry.schema.required:
                        entry.schema.required.append("_meta")
                else:
                    meta = entry.schema.properties["_meta"]
                    
                    # Just in case _meta was somehow not required
                    meta.optional = False
                    props: ObjectType = meta.wrapped
                    props.wrapped["row_id"] = row_id_type
            else:
                # We should never get here, this is just to appease type checking
                raise RuntimeError(f"Something unexpected happened: schema for stream {stream.name} does not have a properties field.", vars(entry.schema))

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
            # have a replication or primary key to use `INCREMENTAL` mode, but it should
            # be possible to "downgrade" an INCREMENTAL stream to FULL_TABLE
            # [1]: https://github.com/meltano/sdk/blob/main/singer_sdk/streams/core.py#L751-L756

            if entry.replication_method == "INCREMENTAL" and entry.replication_key is None:
                raise ValidateError(f"{entry.stream} does not support {entry.replication_method} replication because it does not have a replication key.")

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
        state = t.cast(State, open["state"] or {})

        # Index Key: tap_stream_id.
        # Index Value: [binding index, next row_id].
        (translated_state, index) = flow_to_singer_state(state, open["capture"]["bindings"])

        for i, binding in enumerate(open["capture"]["bindings"]):
            entry = singer.CatalogEntry.from_dict(binding["resourceConfig"])
            entry.schema = singer.Schema.from_dict(binding["collection"]["writeSchema"])
            if not entry.tap_stream_id in index:
                index[entry.tap_stream_id] = [i,1]

            catalog[entry.tap_stream_id] = entry
        
        delegate: singer_sdk.Tap = self.delegate_factory(config=config, catalog=catalog, state=translated_state) # type: ignore
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
                                # Translate back to Flow state keyed on `stateKey`
                                updated=singer_to_flow_state(
                                    state, 
                                    open["capture"]["bindings"], 
                                    # We don't store the binding index in the state because it could change
                                    # Instead we key on the stateKey
                                    {k:v[1] for k,v in index.items()}
                                ),
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
        # "tap_stream_id": {"type": "string"},
        "stream": {"type": "string"},
        "replication_method": {"type": "string", "enum": [REPLICATION_INCREMENTAL, REPLICATION_FULL_TABLE]},
        "replication_key": {"type": "string"}
    },
    "required": ["stream", "replication_method"]
}
