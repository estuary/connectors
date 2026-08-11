from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar
from pydantic import BaseModel, Field


class BaseDocument(BaseModel, extra="allow"):
    class Meta(BaseModel):
        op: Literal["c", "u", "d"] = Field(
            default="u",
            description="Operation type (c: Create, u: Update, d: Delete)",
        )
        row_id: int = Field(
            default=-1,
            description="Row ID of the Document, counting up from zero, or -1 if not known",
        )

        @classmethod
        def sourced_schema(cls) -> dict[str, Any]:
            """The `_meta` sub-schema to nest under `properties` when building a SourcedSchema."""
            return {
                "additionalProperties": False,
                "type": "object",
                "properties": {
                    "op": {
                        "type": "string",
                        "enum": ["c", "u", "d"],
                        "minLength": 1,
                        "maxLength": 1,
                    },
                    "row_id": {"type": "integer", "minimum": 1, "maximum": 1},
                    # Connectors never emit uuid. The runtime adds it to
                    # every captured document.
                    "uuid": {"type": "string", "minLength": 36, "maxLength": 36},
                },
                "required": ["uuid", "op", "row_id"],
            }

    meta_: Meta = Field(
        default_factory=lambda: BaseDocument.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )


_BaseDocument = TypeVar("_BaseDocument", bound=BaseDocument)


@dataclass
class AssociatedDocument(Generic[_BaseDocument]):
    """
    Emitting AssociatedDocument allows you to represent capturing document for other bindings.
    You might use this if your data model requires you to load "child" documents when capturing a "parent" document,
    instead of independently loading the child data stream.
    """

    doc: _BaseDocument
    binding: int
