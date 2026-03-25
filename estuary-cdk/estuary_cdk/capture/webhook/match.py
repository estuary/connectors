from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from typing import Annotated, Literal, cast, override

from aiohttp import web
from aiohttp.web_urldispatcher import DynamicResource
from pydantic import AfterValidator, BaseModel, model_validator
from pydantic.fields import Field

from estuary_cdk.pydantic_polyfill import JsonValue

# ------------------------------------------------------------------------------
# Match types: concrete match rules used for routing incoming webhooks
# to resources.
#
# CollectionMatchingSpec = UrlMatch | HeaderMatch | BodyMatch
# ------------------------------------------------------------------------------


class MatchRule(BaseModel, frozen=True, metaclass=ABCMeta):
    value: str

    @property
    @abstractmethod
    def display_name(self) -> str: ...

    @abstractmethod
    def list_compatibility_errors(
        self, other: CollectionMatchingSpec
    ) -> str | None: ...

    @abstractmethod
    async def matches(self, req: web.Request) -> bool: ...

    @property
    @abstractmethod
    def sort_key(self) -> tuple[int, int]: ...


class UrlMatch(MatchRule, frozen=True):
    type: Literal["url"] = "url"

    @property
    @override
    def display_name(self) -> str:
        return re.sub("[^a-zA-Z]*", "_", self.value)

    @model_validator(mode="after")
    def _validate_url_pattern(self) -> UrlMatch:
        if self.value != "*":
            # Defer to aiohttp's path parsing logic
            _ = DynamicResource(self.value)
        return self

    @staticmethod
    def _is_placeholder_segment(segment: str) -> bool:
        return segment.startswith("{") and segment.endswith("}")

    @override
    def list_compatibility_errors(self, other: CollectionMatchingSpec) -> str | None:
        if not isinstance(other, type(self)):
            return None

        if self.value == "*" and other.value == "*":
            return "The '*' wildcard can only be used in one UrlMatch definition"
        if self.value == other.value:
            return f"Only one UrlMatch can claim the {self.value!r} discriminator value"
        if self.value == "*" or other.value == "*":
            return None

        # "/foo/{x}/baz" and "/foo/bar/{y}" will both match on "/foo/bar/baz".
        # We allow ambiguous paths if one is strictly more specific than the other,
        # but error on equivalent specificity.
        self_segments = self.value.strip("/").split("/")
        other_segments = other.value.strip("/").split("/")

        if len(self_segments) != len(other_segments):
            return None

        is_self_more_specific = False  # ...in at least one segment
        is_other_more_specific = False  # ...in at least one segment

        for self_seg, other_seg in zip(self_segments, other_segments):
            is_self_param = self._is_placeholder_segment(self_seg)
            is_other_param = self._is_placeholder_segment(other_seg)

            if not is_self_param and not is_other_param:
                if self_seg != other_seg:
                    return None
                continue

            if is_self_param and is_other_param:
                continue

            if is_self_param:
                is_other_more_specific = True
            else:
                is_self_more_specific = True

        if is_self_more_specific ^ is_other_more_specific:
            return None

        return f"URL patterns '{self.value}' and '{other.value}' match the same URLs"

    @override
    async def matches(self, req: web.Request) -> bool:
        if self.value == "*":
            return True

        return (
            DynamicResource(self.value)._match(  # pyright: ignore[reportPrivateUsage]
                req.path
            )
            is not None
        )

    @property
    @override
    def sort_key(self) -> tuple[int, int]:
        if self.value == "*":
            return (0, -1)
        segments = self.value.strip("/").split("/")
        return (0, sum(1 for s in segments if not self._is_placeholder_segment(s)))


class _KeyValueMatch(MatchRule, frozen=True, metaclass=ABCMeta):
    key: str

    @property
    @override
    def display_name(self) -> str:
        return self.value

    @override
    def list_compatibility_errors(self, other: CollectionMatchingSpec) -> str | None:
        if not isinstance(other, type(self)):
            return None
        if self.key != other.key:
            return None
        if self.value == other.value:
            return (
                f"Only one {type(self).__name__}(key={self.key!r}) "
                f"can claim the {self.value!r} discriminator value"
            )
        return None


class HeaderMatch(_KeyValueMatch, frozen=True):
    type: Literal["header"] = "header"

    @override
    async def matches(self, req: web.Request) -> bool:
        header_value = req.headers.get(self.key)
        if header_value is None:
            return False
        return header_value == self.value

    @property
    @override
    def sort_key(self) -> tuple[int, int]:
        return (2, 0)


class BodyMatch(_KeyValueMatch, frozen=True):
    type: Literal["body"] = "body"
    key: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

    @override
    async def matches(self, req: web.Request) -> bool:
        if "_parsed_body" not in req:
            # Cache the result to avoid re-parsing on every match rule
            req["_parsed_body"] = await req.json()

        body = cast(dict[str, JsonValue], req["_parsed_body"])

        segments = self.key.split(".")
        for segment in segments[:-1]:
            nested = body.get(segment)
            if not isinstance(nested, dict):
                return False
            body = cast(dict[str, JsonValue], nested)

        value = body.get(segments[-1])
        return value is not None and str(value) == self.value

    @property
    @override
    def sort_key(self) -> tuple[int, int]:
        return (1, 0)


CollectionMatchingSpec = Annotated[
    UrlMatch | HeaderMatch | BodyMatch,
    Field(discriminator="type"),
]


# ------------------------------------------------------------------------------
# Discriminators: used for generating match rule instances.
# ------------------------------------------------------------------------------


def non_empty_set_validator(v: set[str]) -> set[str]:
    if not v:
        raise ValueError("set must not be empty")
    return v


NonEmptyStrSet = Annotated[set[str], AfterValidator(non_empty_set_validator)]


class Discriminator(BaseModel, frozen=True, metaclass=ABCMeta):
    known_values: set[str]

    @abstractmethod
    def for_value(self, value: str) -> CollectionMatchingSpec: ...

    def create_match_rules(self) -> Sequence[CollectionMatchingSpec]:
        return [self.for_value(value) for value in self.known_values]


class UrlDiscriminator(Discriminator, frozen=True):
    type: Literal["url"] = "url"
    known_values: set[str] = set()

    @model_validator(mode="after")
    def _validate_known_values(self) -> UrlDiscriminator:
        # Defer to aiohttp's path parsing logic
        [DynamicResource(v) for v in self.known_values if v != "*"]

        return self

    @override
    def for_value(self, value: str) -> UrlMatch:
        return UrlMatch(type="url", value=value)

    @override
    def create_match_rules(self):
        if not self.known_values:
            return [self.for_value("*")]

        return super().create_match_rules()


class HeaderDiscriminator(Discriminator, frozen=True):
    type: Literal["header"] = "header"
    key: str
    known_values: NonEmptyStrSet

    @override
    def for_value(self, value: str) -> HeaderMatch:
        return HeaderMatch(type="header", key=self.key, value=value)


class BodyDiscriminator(Discriminator, frozen=True):
    type: Literal["body"] = "body"
    key: str  # dot-path into request body
    known_values: NonEmptyStrSet

    @override
    def for_value(self, value: str) -> BodyMatch:
        return BodyMatch(type="body", key=self.key, value=value)


CollectionDiscriminatorSpec = Annotated[
    UrlDiscriminator | HeaderDiscriminator | BodyDiscriminator,
    Field(discriminator="type"),
]
"""Essentially a factory for resource matching rules."""
