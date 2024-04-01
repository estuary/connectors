from datetime import datetime, UTC, timedelta
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Dict, Any, Mapping
from pydantic import BaseModel, Field, AwareDatetime, model_validator
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
#from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding
from estuary_cdk.capture import common
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .api import fetch_custom_objects
from .models import  EndpointConfig, CRMObject, OAUTH2_SPEC, ResourceConfig, ResourceState

from estuary_cdk.capture.common import BaseDocument

async def all_custom_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    all_resources = []

    all_custom_objects = await fetch_custom_objects(log, http)
    if len(all_custom_objects["results"]) == 0:
        return None
   
    for objects in all_custom_objects["results"]:
        CustomObject.NAME = objects["labels"]["plural"]
        CustomObject.PRIMARY_KEY = ["/" + name for name in objects["requiredProperties"]]
        CustomObject.primare = objects["requiredProperties"]
        CustomObject.PROPERTY_SEARCH_NAME = objects["labels"]["plural"]
        schema = generate_schema(objects)

        all_resources.append(custom_objects(CustomObject, http, schema))
        
    return all_resources


def custom_objects(
    cls: type[CRMObject], http: HTTPSession, schema: Dict,
) -> common.Resource:
    
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page, cls, http),
        )
    

    started_at = datetime.now(tz=UTC)
    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=common.Resource.FixedSchema(value=schema),
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=False,
    )


class CustomObject(BaseDocument, extra="allow"):

    NAME: ClassVar[str]
    PRIMARY_KEY: ClassVar[list[str]]
    PROPERTY_SEARCH_NAME: ClassVar[str]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False

def generate_schema(raw_schema: Dict) -> Dict:
    properties = {}
    for field in raw_schema["properties"]:
        properties[field["name"]] = field_to_property_schema(field)

    schema = {
        "$defs": {
            "Meta": {
            "properties": {
                "op": {
                "description": "Operation type (c: Create, u: Update, d: Delete)",
                "enum": [
                    "c",
                    "u",
                    "d"
                ],
                "title": "Op",
                "type": "string"
                },
                "row_id": {
                "default": -1,
                "description": "Row ID of the Document, counting up from zero, or -1 if not known",
                "title": "Row Id",
                "type": "integer"
                }
            },
            "required": [
                "op"
            ],
            "title": "Meta",
            "type": "object"
            }
        },
        "type": "object",
        "title": raw_schema["labels"]["plural"],
        "additionalProperties": True,
        "properties": {
            "createdAt": {"type": ["null", "string"], "format": "date-time"},
            "updatedAt": {"type": ["null", "string"], "format": "date-time"},
            "archived": {"type": ["null", "boolean"]},
            "properties": {"type": ["null", "object"], "properties": properties},
        },
            "required": raw_schema["requiredProperties"]
    }
    for prop in raw_schema["requiredProperties"]:
        schema["properties"][f"{prop}"] = {"type":["string"]}

    return schema

def field_to_property_schema(field: Dict[str, Any]) -> Mapping[str, Any]:
    field_type = field["type"]
    property_schema = {}
    if field_type == "enumeration" or field_type == "string":
        property_schema = {"type": ["null", "string"]}
    elif field_type == "datetime" or field_type == "date":
        property_schema = {"type": ["null", "string"], "format": "date-time"}
    elif field_type == "number":
        property_schema = {"type": ["null", "number"]}
    elif field_type == "boolean" or field_type == "bool":
        property_schema = {"type": ["null", "boolean"]}
    else:
        property_schema = {"type": ["null", "string"]}

    return property_schema