"""
Feature flags, set per task in the `/advanced/feature_flags` property of the
endpoint config. This mirrors go/common/feature_flags.go so that flags behave the
same way in Python and Go connectors. See docs/feature_flags.md.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


@dataclass(frozen=True)
class FlagDefault:
    """
    FlagDefault is the default value of a feature flag, which is either fixed or
    depends on when the task was created. A `flag` or `no_flag` entry in the
    config always wins over it.
    """

    value: bool = False
    enabled_from: date | None = None
    """When set, tasks created on or after this date default to true and older
    tasks default to false."""

    @classmethod
    def enabled_for_tasks_created_after(cls, cutoff: date) -> "FlagDefault":
        """
        A default that changes behavior for new tasks only. Set the cutoff to a
        date after the change is merged. That prevents race conditions that could cause
        existing tasks to use the new, feature flagged behavior.
        Users can still opt either way per task.
        """
        return cls(enabled_from=cutoff)

    def resolve(self, created_at: date | None) -> bool:
        if self.enabled_from is None:
            return self.value

        # A task without a creation date is being created right now.
        return (created_at or datetime.now(tz=UTC).date()) >= self.enabled_from


FLAG_ENABLED = FlagDefault(value=True)
FLAG_DISABLED = FlagDefault(value=False)


def parse_feature_flags(raw: str, defaults: dict[str, bool]) -> dict[str, bool]:
    """
    Layer the comma-separated flags of `raw` over `defaults`. A flag prefixed
    with `no_` is set to false.
    """
    settings = dict(defaults)
    for name in raw.split(","):
        name = name.strip()
        value = not name.startswith("no_")
        name = name.removeprefix("no_")
        if name:
            settings[name] = value
    return settings


# pydantic passes the generated schema of each FeatureFlagsConfig subclass here.
# Popping feature_flags out of the properties and assigning it back moves it to the
# end, since dicts keep insertion order. The UI lists properties that have an `order`
# first and the rest in schema order, so feature_flags displays last as long as it
# has no `order` of its own.
#
# NOTE: We can't use the precise type JsonDict here until all connectors
# are no longer using pydantic v1 - JsonDict was introduced in pydantic v2.
def _list_feature_flags_last(schema: dict[str, Any], _: type) -> None:
    properties = schema["properties"]
    properties["feature_flags"] = properties.pop("feature_flags")


class FeatureFlagsConfig(BaseModel):
    """
    FeatureFlagsConfig is the base class for a connector's `advanced` config
    model, which is where support and the bulk-editing scripts set feature flags.

    Declare every flag the connector supports in FEATURE_FLAG_DEFAULTS, with a
    short comment saying what it does. The CDK resolves the flags before it hands
    each request to the connector.
    """

    FEATURE_FLAG_DEFAULTS: ClassVar[dict[str, FlagDefault]] = {}

    # Display feature flags last in the advanced endpoint configuration section by default.
    # Connectors can override this if they really want by setting json_schema_extra on
    # their subclass.
    model_config = ConfigDict(json_schema_extra=_list_feature_flags_last)

    feature_flags: str = Field(
        default="",
        title="Feature Flags",
        description="This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support.",
        json_schema_extra={"nonsensitive": True},
    )

    _resolved_flags: dict[str, bool] | None = PrivateAttr(default=None)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        if "FEATURE_FLAG_DEFAULTS" in cls.model_fields:
            raise TypeError(
                f"Implementation error: {cls.__name__}.FEATURE_FLAG_DEFAULTS must be annotated as a ClassVar"
            )
        for name in cls.FEATURE_FLAG_DEFAULTS:
            if name.startswith("no_"):
                raise ValueError(
                    f"Implementation error: feature flag '{name}' can't start with 'no_'"
                )

    def resolve_feature_flags(self, created_at: date | None) -> dict[str, bool]:
        defaults = {
            name: default.resolve(created_at)
            for name, default in self.FEATURE_FLAG_DEFAULTS.items()
        }
        self._resolved_flags = parse_feature_flags(self.feature_flags, defaults)
        return self._resolved_flags

    def flag(self, name: str) -> bool:
        if name not in self.FEATURE_FLAG_DEFAULTS:
            raise KeyError(
                f"Implementation error: feature flag '{name}' has no default in FEATURE_FLAG_DEFAULTS"
            )
        if self._resolved_flags is None:
            raise RuntimeError(
                f"Implementation error: feature flag '{name}' was read before feature flags were resolved"
            )
        return self._resolved_flags[name]
