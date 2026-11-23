"""Checks that discovered schemas are ones Flow can publish."""

from collections.abc import Iterable, Iterator
from typing import Any, cast

from pydantic_core import SchemaError, SchemaValidator, core_schema

# Keywords whose values Flow builds as subschemas. Flow never compiles a
# `pattern` outside of these, such as one inside `default` or an `x-` keyword.
_SUBSCHEMA_KEYWORDS = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "allOf",
        "anyOf",
        "contains",
        "else",
        "if",
        "items",
        "not",
        "oneOf",
        "prefixItems",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)
_SUBSCHEMA_MAP_KEYWORDS = frozenset(
    {"$defs", "definitions", "dependentSchemas", "patternProperties", "properties"}
)

# How many locations an error lists for each regex that doesn't compile.
_MAX_REGEX_LOCATIONS = 5


def check_schema_regexes(schemas: Iterable[tuple[str, dict[str, Any]]]) -> None:
    """Raises if any of `schemas` has a regex that Flow can't compile.

    A regex can repeat across many fields, like pydantic's `Decimal` one, so
    each regex is compiled and reported once.
    """
    locations: dict[str, list[str]] = {}
    for name, schema in schemas:
        for pointer, regex in _schema_regexes(schema):
            locations.setdefault(regex, []).append(f"{name} {pointer}")

    errors: list[str] = []
    for regex, regex_locations in locations.items():
        error = _regex_error(regex)
        if error is None:
            continue

        shown = ", ".join(regex_locations[:_MAX_REGEX_LOCATIONS])
        if len(regex_locations) > _MAX_REGEX_LOCATIONS:
            shown += f", and {len(regex_locations) - _MAX_REGEX_LOCATIONS} more"
        errors.append(f"`{regex}` at {shown}: {error}")

    if errors:
        raise RuntimeError(
            "Implementation error: discovered schemas contain regexes that Flow can't compile:\n"
            + "\n".join(errors)
        )


def _schema_regexes(schema: Any, pointer: str = "") -> Iterator[tuple[str, str]]:
    """Yields the JSON pointer and regex of each `pattern` value and
    `patternProperties` key in `schema` and its subschemas."""
    if not isinstance(schema, dict):
        return
    schema = cast(dict[str, Any], schema)

    for keyword, value in schema.items():
        location = f"{pointer}/{keyword}"
        if keyword == "pattern" and isinstance(value, str):
            yield location, value
        elif (
            keyword == "additionalItems"
            and "items" in schema
            and not isinstance(schema["items"], list)
        ):
            # Flow ignores `additionalItems` when `items` is a single schema.
            continue
        elif keyword in _SUBSCHEMA_MAP_KEYWORDS and isinstance(value, dict):
            for name, subschema in cast(dict[str, Any], value).items():
                token = name.replace("~", "~0").replace("/", "~1")
                if keyword == "patternProperties":
                    yield f"{location}/{token}", name
                yield from _schema_regexes(subschema, f"{location}/{token}")
        elif keyword in _SUBSCHEMA_KEYWORDS:
            if isinstance(value, list):
                for index, subschema in enumerate(cast(list[Any], value)):
                    yield from _schema_regexes(subschema, f"{location}/{index}")
            else:
                yield from _schema_regexes(value, location)


def _regex_error(regex: str) -> str | None:
    """Returns the `regex` crate's error for the given regex, or None if it compiles.

    Flow compiles schema regexes with Rust's `regex` crate. pydantic-core's
    `rust-regex` engine uses the same crate, so it rejects the same regexes.
    """
    try:
        SchemaValidator(core_schema.str_schema(pattern=regex, regex_engine="rust-regex"))
    except SchemaError as err:
        # The crate's own message is the last line.
        return str(err).splitlines()[-1]
    return None
