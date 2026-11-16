"""
Backfill signals are the BackfillBegin and BackfillComplete messages of the capture
protocol. A backfill sent with them restates its binding: destinations delete every
row that it and the incremental stream don't capture again between the two signals.

A resource declares the coverage of its backfills, and the CDK sends a backfill as a
restatement only when that coverage is FULL, or BOUNDED_BY_CONFIG while
`ignore_config_for_backfill_signals` is on. A connector opts a resource in by setting
`backfill_coverage` on it. The connector's `advanced` config also declares the flags
that turn signals on and off.

    class Advanced(FeatureFlagsConfig):
        FEATURE_FLAG_DEFAULTS: ClassVar[dict[str, FlagDefault]] = {
            # The day after the connector starts sending signals.
            **backfill_signal_flag_defaults(enabled_for_tasks_created_after=date(...)),
        }

    common.Resource(..., backfill_coverage=BackfillCoverage.BOUNDED_BY_CONFIG)

A resource whose answer depends on the binding or on whether the backfill is scheduled
passes a function of a BackfillContext instead.

A resource that signals has to pass each binding's backfill `fetch_page` to
`open_binding`, since a custom backfill loop can't finish a signaled backfill. The CDK
raises before sending a BackfillBegin otherwise, so answer PARTIAL for a binding that
backfills any other way.
"""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Callable, Generic

from ..feature_flags import FLAG_DISABLED, FlagDefault
from ..flow import CaptureBinding, ResourceConfig

EMIT_BACKFILL_SIGNALS = "emit_backfill_signals"
"""Sends backfill signals for backfills with FULL coverage."""

IGNORE_CONFIG_FOR_BACKFILL_SIGNALS = "ignore_config_for_backfill_signals"
"""Also sends backfill signals for BOUNDED_BY_CONFIG backfills. A user who sets it
accepts that destinations delete the rows outside the configured bounds."""


class BackfillCoverage(StrEnum):
    """
    How much of its source a backfill and the resource's incremental stream recapture
    between the BackfillBegin and the BackfillComplete. If in doubt, answer PARTIAL.

    A backfill doesn't have FULL coverage when any of these conditions are true:
    - A bound limits what it reads, like a start date, a lookback window, or a project
      filter. A bound from the configuration makes it BOUNDED_BY_CONFIG even at its
      default value, unless that default provably excludes nothing.
    - The source deletes records on its own schedule through retention, expiry, or a
      purge of soft-deleted records. Destinations would lose the rows of records the
      source no longer keeps, and that bound isn't configuration, so it makes the
      backfill PARTIAL.
    - The emitted documents omit fields and rely on a merge reduction strategy to merge
      them into previously captured documents. A BackfillBegin prevents those
      documents from being reduced into the previously captured ones and
      materialized documents lose the omitted fields.

    A connector that captures many kinds of objects answers PARTIAL by default and
    opts in only the objects it has checked.
    """

    FULL = "full"
    """Recaptures every record in the source in full. Sends signals while
    emit_backfill_signals is on."""

    BOUNDED_BY_CONFIG = "bounded_by_config"
    """Full, apart from bounds or filters from the configuration. Sends signals only
    while ignore_config_for_backfill_signals is also on."""

    PARTIAL = "partial"
    """Anything else, including "not sure". Never sends signals."""


@dataclass(frozen=True)
class BackfillContext(Generic[ResourceConfig]):
    """
    What the CDK knows about a backfill when it asks a resource for the backfill's
    coverage. The CDK asks before the backfill begins and again at every
    Open until it sends the BackfillComplete. An answer that stops qualifying drops that
    BackfillComplete, so the answer has to depend only on what stays fixed during a
    backfill, like the configuration, discovered metadata, and `is_connector_initiated`.
    A resource that needs the endpoint config closes over it when it's built.

    The CDK passes this as a single argument so it can add fields later without
    breaking existing functions.
    """

    binding: CaptureBinding[ResourceConfig]

    is_connector_initiated: bool


BackfillCoverageFn = Callable[[BackfillContext[ResourceConfig]], BackfillCoverage]
"""A resource's answer for each of its backfills, for a resource whose answer depends
on the binding or on whether the backfill is scheduled."""


def backfill_signal_flag_defaults(
    *, enabled_for_tasks_created_after: date
) -> dict[str, FlagDefault]:
    """
    The FEATURE_FLAG_DEFAULTS entries for backfill signals. emit_backfill_signals
    defaults on for tasks created on or after the given date. Pass a date after the
    release that sends signals is deployed to avoid a race between the deployment
    and existing tasks starting to send backfill signals.
    """
    return {
        EMIT_BACKFILL_SIGNALS: FlagDefault.enabled_for_tasks_created_after(
            enabled_for_tasks_created_after
        ),
        IGNORE_CONFIG_FOR_BACKFILL_SIGNALS: FLAG_DISABLED,
    }
