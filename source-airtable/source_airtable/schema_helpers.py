#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from copy import deepcopy
from typing import Any, Dict

from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode

logger: logging.Logger = logging.getLogger("airbyte")


class SchemaTypes:

    string: Dict = {"type": ["null", "string"]}

    number: Dict = {"type": ["null", "number", "string"], "format": "number"}

    boolean: Dict = {"type": ["null", "boolean"]}

    date: Dict = {"type": ["null", "string"], "format": "date"}

    datetime: Dict = {"type": ["null", "string"], "format": "date-time"}

    array_with_strings: Dict = {"type": ["null", "array"], "items": {"type": ["null", "string"]}}

    # array items should be automatically determined
    # based on field complexity
    array_with_any: Dict = {"type": ["null", "array"], "items": {}}


# More info about internal Airtable Data Types
# https://airtable.com/developers/web/api/field-model
SIMPLE_AIRTABLE_TYPES: Dict = {
    "multipleAttachments": SchemaTypes.string,
    "autoNumber": SchemaTypes.number,
    "barcode": SchemaTypes.string,
    "button": SchemaTypes.string,
    "checkbox": SchemaTypes.boolean,
    "singleCollaborator": SchemaTypes.string,
    "count": SchemaTypes.number,
    "createdBy": SchemaTypes.string,
    "createdTime": SchemaTypes.datetime,
    "currency": SchemaTypes.number,
    "email": SchemaTypes.string,
    "date": SchemaTypes.date,
    "dateTime": SchemaTypes.datetime,
    "duration": SchemaTypes.number,
    "lastModifiedBy": SchemaTypes.string,
    "lastModifiedTime": SchemaTypes.datetime,
    "multipleRecordLinks": SchemaTypes.array_with_strings,
    "multilineText": SchemaTypes.string,
    "multipleCollaborators": SchemaTypes.array_with_strings,
    "multipleSelects": SchemaTypes.array_with_strings,
    "number": SchemaTypes.number,
    "percent": SchemaTypes.number,
    "phoneNumber": SchemaTypes.string,
    "rating": SchemaTypes.number,
    "richText": SchemaTypes.string,
    "singleLineText": SchemaTypes.string,
    "singleSelect": SchemaTypes.string,
    "externalSyncSource": SchemaTypes.string,
    "url": SchemaTypes.string,
    # referral default type
    "simpleText": SchemaTypes.string,
}

# returns the `array of Any` where Any is based on Simple Types.
# the final array is fulled with some simple type.
COMPLEX_AIRTABLE_TYPES: Dict = {
    "formula": SchemaTypes.array_with_any,
    "lookup": SchemaTypes.array_with_any,
    "multipleLookupValues": SchemaTypes.array_with_any,
    "rollup": SchemaTypes.array_with_any,
}

ARRAY_FORMULAS = ("ARRAYCOMPACT", "ARRAYFLATTEN", "ARRAYUNIQUE", "ARRAYSLICE")


class SchemaHelpers:
    @staticmethod
    def clean_name(name_str: str) -> str:
        return name_str.replace(" ", "_").lower().strip()

    @staticmethod
    def get_json_schema(table: Dict[str, Any]) -> Dict[str, str]:
        properties: Dict = {
            # This is primary key, shouldn't be null
            "_airtable_id": {"type": "string"},
            "_airtable_created_time": SchemaTypes.string,
            "_airtable_table_name": SchemaTypes.string,
        }

        json_schema: Dict = {
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
            "required": ["_airtable_id"]
        }

        return json_schema

    @staticmethod
    def get_airbyte_stream(stream_name: str, json_schema: Dict[str, Any]) -> AirbyteStream:
        return AirbyteStream(
            name=stream_name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            supported_destination_sync_modes=[DestinationSyncMode.overwrite, DestinationSyncMode.append_dedup],
            source_defined_primary_key=[["_airtable_id"]]
        )
