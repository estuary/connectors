import functools
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_campaigns,
    backfill_email_activity,
    backfill_list_children,
    fetch_campaigns,
    fetch_email_activity,
    fetch_list_children,
    fetch_parent_ids,
    resolve_base_url,
    snapshot_automations,
    snapshot_children,
    snapshot_interests,
    snapshot_lists,
    snapshot_segment_members,
)
from .models import (
    OAUTH2_SPEC,
    SNAPSHOT_CHILD_STREAMS,
    Automation,
    Campaign,
    ConnectorState,
    EmailActivityEvent,
    EndpointConfig,
    Interest,
    ListMember,
    MailchimpEntity,
    MailchimpIncrementalChildEntity,
    MailchimpList,
    ParentId,
    Segment,
    SegmentMember,
)

DEFAULT_SCHEDULE = "0 0 * * *"

MailchimpResource = Resource[BaseDocument, ResourceConfigWithSchedule, ResourceState]

# Top-level collections captured as snapshots: small, mutable, and without any
# updated_at-style cursor field, so periodic full re-lists with tombstoned
# deletions beat a created-time cursor that can never observe updates.
SNAPSHOT_RESOURCES: list[type[MailchimpEntity]] = [
    MailchimpList,
    Automation,
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        base_url = await resolve_base_url(log, http, config.credentials)
        _ = await http.request(log, f"{base_url}/ping")
    except ValueError as err:
        raise ValidationError([str(err)])
    except HTTPError as err:
        if err.code == 401:
            msg = (
                "Invalid credentials. Please confirm the provided credentials "
                f"are correct.\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def snapshot_resources(http: HTTPMixin, base_url: str) -> list[MailchimpResource]:
    """Return Resource objects for all snapshot (full-refresh) streams."""

    snapshot_fetchers = {
        MailchimpList.NAME: functools.partial(snapshot_lists, http, base_url),
        Automation.NAME: functools.partial(snapshot_automations, http, base_url),
        Interest.NAME: functools.partial(snapshot_interests, http, base_url),
        SegmentMember.NAME: functools.partial(snapshot_segment_members, http, base_url),
        **{
            spec.model.NAME: functools.partial(snapshot_children, spec, http, base_url)
            for spec in SNAPSHOT_CHILD_STREAMS
        },
    }

    def open(
        resource_name: str,
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fetchers[resource_name],
        )

    return [
        SnapshotResource(
            name=model.NAME,
            open=functools.partial(open, model.NAME),
            initial_config=ResourceConfigWithSchedule(
                name=model.NAME, interval=timedelta(hours=1)
            ),
        )
        for model in [
            *SNAPSHOT_RESOURCES,
            Interest,
            SegmentMember,
            *(spec.model for spec in SNAPSHOT_CHILD_STREAMS),
        ]
    ]


def campaigns(
    http: HTTPMixin, base_url: str, config: EndpointConfig
) -> MailchimpResource:
    """Return Resource for incremental + backfill campaigns capture."""
    cutoff = datetime.now(tz=UTC)

    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_campaigns, http, base_url),
            fetch_page=functools.partial(
                backfill_campaigns, http, base_url, config.start_date
            ),
        )

    return MailchimpResource(
        name=Campaign.NAME,
        key=["/id"],
        model=Campaign,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=Campaign.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE,
        ),
        schema_inference=True,
    )


def email_activity(
    http: HTTPMixin, base_url: str, config: EndpointConfig
) -> MailchimpResource:
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_email_activity, http, base_url, config.start_date
            ),
            fetch_page=functools.partial(
                backfill_email_activity, http, base_url, config.start_date
            ),
        )

    return MailchimpResource(
        name=EmailActivityEvent.NAME,
        key=["/_meta/campaign_id", "/_meta/email_id", "/action", "/timestamp"],
        model=EmailActivityEvent,
        open=open,
        initial_state=ResourceState(
            # cutoff − 1s collects only complete seconds
            inc=ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1)),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=EmailActivityEvent.NAME,
            interval=timedelta(minutes=20),
        ),
        schema_inference=True,
    )


async def _patch_missing_subtask_states(
    binding: CaptureBinding[ResourceConfigWithSchedule],
    state: ResourceState,
    task: Task,
    keys: list[str],
    cutoff: datetime,
):
    """List IDs resolve at connector startup, so a list created after the last
    checkpoint has no subtask entries in the recovered state. Seed fresh ones
    (a full backfill up to `cutoff`, incremental from there) and checkpoint so
    they persist."""
    if not (isinstance(state.inc, dict) and isinstance(state.backfill, dict)):
        return

    missing = [key for key in keys if key not in state.inc]
    if not missing:
        return

    new_states = ResourceState(
        inc={
            # cutoff − 1s: the first incremental poll then emits from
            # `cutoff` onward, exactly one second after backfill's last
            # covered second (`cutoff − 1s`) — no gap, no overlap.
            key: ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1))
            for key in missing
        },
        backfill={
            key: ResourceState.Backfill(cutoff=cutoff, next_page=None)
            for key in missing
        },
    )
    assert isinstance(new_states.inc, dict)
    assert isinstance(new_states.backfill, dict)

    state.inc.update(new_states.inc)
    state.backfill.update(new_states.backfill)

    task.log.info(
        f"Checkpointing state to persist new subtasks for {binding.stateKey}.",
        {"new_subtasks": missing},
    )
    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


# Sweep params shared by both list_members subtasks. The ASC sort makes the
# walk order deterministic among rows still in the window — it does NOT make
# positions stable (an update moves a row out of a frozen window entirely,
# renumbering the tail), which is why backfills never checkpoint an offset
# across invocations.
_MEMBER_SORT: dict[str, str | int] = {
    "sort_field": "last_changed",
    "sort_dir": "ASC",
}


@dataclass(frozen=True)
class _ListChildSubtask:
    """One (list, sweep) unit of an incremental list-child stream; the
    fetcher/state dicts key each instance by its subtask key."""

    list_id: ParentId
    # Sweep params merged into every page request (e.g. the archived members
    # sweep's `status` filter).
    request_params: dict[str, str | int]


def incremental_list_children(
    http: HTTPMixin, base_url: str, config: EndpointConfig, list_ids: list[ParentId]
) -> list[MailchimpResource]:
    """Return Resources for the incremental list-children streams
    (list_members, segments).

    Each stream fans out into per-(list, sweep) subtasks, every subtask with
    its own incremental cursor and windowed backfill. list_members needs
    two sweeps per list because the unfiltered listing silently excludes
    archived members — and the sweeps' cursors must be independent: a member
    unarchived between the two sweeps of a shared-cursor walk would be missed
    until its next change, while a per-sweep cursor only ever advances past
    docs its own sweep observed. segments' bare listing is the full population
    (segments have no archived state), so it gets one sweep per list."""

    # Floored to the second so the backfill/incremental boundary sits on a
    # complete-second edge, matching the fetchers' whole-second windows.
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    member_subtasks: dict[str, _ListChildSubtask] = {}
    for list_id in list_ids:
        member_subtasks[f"{list_id}.default"] = _ListChildSubtask(
            list_id, {**_MEMBER_SORT}
        )
        member_subtasks[f"{list_id}.archived"] = _ListChildSubtask(
            list_id, {**_MEMBER_SORT, "status": "archived"}
        )

    segment_subtasks = {
        str(list_id): _ListChildSubtask(list_id, {}) for list_id in list_ids
    }

    def resource[T: MailchimpIncrementalChildEntity](
        model: type[T],
        subtasks: dict[str, _ListChildSubtask],
    ) -> MailchimpResource:
        incremental_fetchers = {
            key: functools.partial(
                fetch_list_children,
                http,
                base_url,
                model,
                subtask.list_id,
                subtask.request_params,
            )
            for key, subtask in subtasks.items()
        }
        backfill_fetchers = {
            key: functools.partial(
                backfill_list_children,
                http,
                base_url,
                model,
                subtask.list_id,
                subtask.request_params,
                config.start_date,
            )
            for key, subtask in subtasks.items()
        }

        async def open(
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
        ):
            await _patch_missing_subtask_states(
                binding, state, task, list(subtasks), cutoff
            )

            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=incremental_fetchers,
                fetch_page=backfill_fetchers,
            )

        return MailchimpResource(
            name=model.NAME,
            # Member ids are the MD5 of the lowercase email — unique only
            # within a list — so both streams key on the composite.
            key=["/list_id", "/id"],
            model=model,
            open=open,
            initial_state=ResourceState(
                inc={
                    # cutoff − 1s collects only complete seconds
                    key: ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1))
                    for key in subtasks
                },
                backfill={
                    key: ResourceState.Backfill(cutoff=cutoff, next_page=None)
                    for key in subtasks
                },
            ),
            initial_config=ResourceConfigWithSchedule(
                name=model.NAME, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )

    return [
        resource(ListMember, member_subtasks),
        resource(Segment, segment_subtasks),
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[MailchimpResource]:
    """Return all resources for the Mailchimp connector."""
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )
    base_url = await resolve_base_url(log, http, config.credentials)
    list_ids = await fetch_parent_ids(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, log
    )

    return [
        *snapshot_resources(http, base_url),
        campaigns(http, base_url, config),
        email_activity(http, base_url, config),
        *incremental_list_children(http, base_url, config, list_ids),
    ]
