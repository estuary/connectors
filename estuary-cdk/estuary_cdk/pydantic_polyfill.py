"""
This module is a polyfill which patches pydantic v1 to have the essential
API surface area that estuary-cdk requires of pydantic v2.

This is *mostly* transparent. The one bit of fallout is that pydantic v1
has a GenericModel type which is used a base class, which was removed in
pydantic v2.

This polyfill thus exports a GenericModel class which is simply BaseModel
when pydantic v2 is active.
"""

from typing import TypeVar, TypeAlias
import pydantic
import datetime

try:
    import pydantic.v1

    VERSION = "v2"

    # GenericModel was removed. It's just BaseModel now.
    GenericModel : TypeAlias = pydantic.BaseModel

except ImportError:
    VERSION = "v1"

    import orjson
    from pydantic.generics import GenericModel

    # AwareDateTime was added in V2. Fallback to a regular datetime.
    pydantic.AwareDatetime = datetime.datetime

    def model_dump_json(
        self, by_alias: bool = False, exclude_unset: bool = False
    ) -> str:
        return self.json(by_alias=by_alias, exclude_unset=exclude_unset)

    pydantic.BaseModel.model_dump_json = model_dump_json

    _Model = TypeVar("_Model", bound=pydantic.BaseModel)

    @classmethod
    def model_validate_json(cls: type[_Model], json_data: str | bytes) -> _Model:
        return cls.parse_obj(orjson.loads(json_data))

    setattr(pydantic.BaseModel, "model_validate_json", model_validate_json)

    @classmethod
    def model_json_schema(cls: type[_Model]) -> dict:
        return cls.schema(by_alias=True)

    setattr(pydantic.BaseModel, "model_json_schema", model_json_schema)

__all__ = ["VERSION", "GenericModel"]
