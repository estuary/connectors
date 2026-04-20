from __future__ import annotations

import abc
import re
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, Literal, override

from aiohttp import web
from aiohttp.web_urldispatcher import DynamicResource
from pydantic import BaseModel
from pydantic.fields import Field

from estuary_cdk.pydantic_polyfill import VERSION as _PYDANTIC_VERSION

_BODY_KEY_REGEX = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"
_REGEX_FIELD_KWARG = "pattern" if _PYDANTIC_VERSION == "v2" else "regex"

# ------------------------------------------------------------------------------
# Match types: concrete match rules used for routing incoming webhooks
# to resources.
#
# CollectionMatchingSpec = UrlMatch | HeaderMatch | BodyMatch
# ------------------------------------------------------------------------------


class MatchRule(abc.ABC, BaseModel, frozen=True):
    value: str

    @property
    @abstractmethod
    def display_name(self) -> str: ...

    @abstractmethod
    def list_compatibility_errors(
        self, other: CollectionMatchingSpec
    ) -> str | None: ...

    @abstractmethod
    async def matches(
        self, req: web.Request, raw_doc: dict[str, Any] | None = None
    ) -> bool: ...

    @property
    @abstractmethod
    def sort_key(self) -> tuple[int, int]: ...


class UrlMatch(MatchRule, frozen=True):
    type: Literal["url"] = "url"

    @property
    @override
    def display_name(self) -> str:
        if self.value == "*":
            return "all"

        return re.sub("[^a-zA-Z0-9]+", "_", self.value).strip("_")

    def __init__(self, **data):
        super().__init__(**data)

        if self.value != "*":
            try:
                # Defer to aiohttp's URL parsing logic
                _ = DynamicResource(self.value)
            except (ValueError, AssertionError) as e:
                raise ValueError(f"Invalid URL pattern {self.value!r}: {e}") from e

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
    async def matches(
        self, req: web.Request, raw_doc: dict[str, Any] | None = None
    ) -> bool:
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


class _KeyValueMatch(MatchRule, frozen=True):
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
    async def matches(
        self, req: web.Request, raw_doc: dict[str, Any] | None = None
    ) -> bool:
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
    key: str = Field(**{_REGEX_FIELD_KWARG: _BODY_KEY_REGEX})

    @override
    async def matches(
        self, req: web.Request, raw_doc: dict[str, Any] | None = None
    ) -> bool:
        if raw_doc is None:
            parsed = await req.json()

            if isinstance(parsed, list):
                raise ValueError(
                    "BodyMatch requires raw_doc when the request contains multiple documents"
                )
            if not isinstance(parsed, dict):
                raise ValueError(f"Invalid document type: {type(parsed)}")

            raw_doc = parsed

        cursor: dict[str, Any] = raw_doc
        segments = self.key.split(".")

        for segment in segments[:-1]:
            nested = cursor.get(segment)
            if not isinstance(nested, dict):
                return False

            cursor = nested

        value = cursor.get(segments[-1])
        return value is not None and str(value) == self.value

    @property
    @override
    def sort_key(self) -> tuple[int, int]:
        return (1, 0)


CollectionMatchingSpec = UrlMatch | HeaderMatch | BodyMatch
"""Discriminated union of match rules. Fields of this type must pass
``discriminator="type"`` to ``Field(...)``; the discriminator is kept on the
field rather than in an ``Annotated[..., Field(...)]`` wrapper so that this
alias stays compatible with both Pydantic v1 and v2."""


# ------------------------------------------------------------------------------
# Discriminators: used for generating match rule instances.
# ------------------------------------------------------------------------------


class Discriminator(abc.ABC, BaseModel, frozen=True):
    known_values: set[str]

    @abstractmethod
    def for_value(self, value: str) -> CollectionMatchingSpec: ...

    def create_match_rules(self) -> Sequence[CollectionMatchingSpec]:
        return [self.for_value(value) for value in self.known_values]


class UrlDiscriminator(Discriminator, frozen=True):
    type: Literal["url"] = "url"
    known_values: set[str] = set()

    def __init__(self, **data):
        super().__init__(**data)

        for v in self.known_values:
            if v != "*":
                try:
                    # Defer to aiohttp's URL parsing logic
                    DynamicResource(v)
                except (ValueError, AssertionError) as e:
                    raise ValueError(f"Invalid URL pattern {v!r}: {e}") from e

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
    known_values: set[str] = Field(min_length=1)

    @override
    def for_value(self, value: str) -> HeaderMatch:
        return HeaderMatch(type="header", key=self.key, value=value)


class BodyDiscriminator(Discriminator, frozen=True):
    type: Literal["body"] = "body"
    key: str  # dot-path into request body
    known_values: set[str] = Field(min_length=1)

    @override
    def for_value(self, value: str) -> BodyMatch:
        return BodyMatch(type="body", key=self.key, value=value)


CollectionDiscriminatorSpec = UrlDiscriminator | HeaderDiscriminator | BodyDiscriminator
"""Discriminated union of discriminator factories. Fields of this type must pass
``discriminator="type"`` to ``Field(...)``."""
