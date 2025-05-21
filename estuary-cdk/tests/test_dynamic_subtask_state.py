import logging
import sys
from copy import deepcopy
from datetime import UTC, datetime
from logging import Logger
from unittest.mock import MagicMock

import pytest
from estuary_cdk.capture.common import (
    ResourceState,
    handle_dynamic_subtask_state,
)


@pytest.fixture
def real_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    # Avoid duplicate handlers
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(handler)
    return logger


@pytest.fixture
def mock_logger():
    return MagicMock(spec=Logger)


def initial_state_without_subtasks():
    started_at = datetime.now(tz=UTC)

    return ResourceState(
        inc=ResourceState.Incremental(cursor=started_at),
        backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
    )


def initial_state_with_subtasks():
    started_at = datetime.now(tz=UTC)

    return ResourceState(
        inc={
            "realtime": ResourceState.Incremental(cursor=started_at),
            "lookback": ResourceState.Incremental(cursor=started_at),
        },
        backfill={
            "realtime": ResourceState.Backfill(next_page=None, cutoff=started_at),
            "lookback": ResourceState.Backfill(next_page=None, cutoff=started_at),
        },
    )


def fetch_changes_with_subtasks(new_subtask_id: str | None = None):
    async def fetch_changes_fn(*args, **kwargs):
        yield {"data": "mocked data"}

    subtask_ids = ["realtime", "lookback"]
    if new_subtask_id:
        subtask_ids.append(new_subtask_id)

    return {subtask_id: fetch_changes_fn for subtask_id in subtask_ids}


def fetch_changes_without_subtasks():
    async def fetch_changes_fn(*args, **kwargs):
        yield {"data": "mocked data"}

    return fetch_changes_fn


def fetch_page_with_subtasks(new_subtask_id: str | None = None):
    async def fetch_page_fn(*args, **kwargs):
        yield {"data": "mocked page data"}

    subtask_ids = ["realtime", "lookback"]
    if new_subtask_id:
        subtask_ids.append(new_subtask_id)

    return {subtask_id: fetch_page_fn for subtask_id in subtask_ids}


def fetch_page_without_subtasks():
    async def fetch_page_fn(*args, **kwargs):
        yield {"data": "mocked page data"}

    return fetch_page_fn


def initialize_subtask_state(log, state, subtask_id) -> ResourceState:
    if isinstance(state.inc, dict) and subtask_id not in state.inc:
        state.inc[subtask_id] = ResourceState.Incremental(cursor=datetime.now(tz=UTC))
    if isinstance(state.backfill, dict) and subtask_id not in state.backfill:
        state.backfill[subtask_id] = ResourceState.Backfill(
            next_page=None, cutoff=datetime.now(tz=UTC)
        )

    return state


@pytest.mark.parametrize(
    "initial_state, fetch_changes, fetch_page, expect_exception, expect_state_change",
    [
        (
            initial_state_without_subtasks(),
            fetch_changes_without_subtasks(),
            fetch_page_without_subtasks(),
            False,
            False,
        ),
        (
            initial_state_without_subtasks(),
            fetch_changes_without_subtasks(),
            None,
            False,
            False,
        ),
    ],
)
def test_handle_dynamic_subtask_without_subtask_state(
    initial_state,
    fetch_changes,
    fetch_page,
    expect_exception,
    expect_state_change,
    mock_logger,
):
    if expect_exception:
        with pytest.raises(ValueError):
            handle_dynamic_subtask_state(
                log=mock_logger,
                resource_state=initial_state,
                fetch_changes=fetch_changes,
                fetch_page=fetch_page,
            )
        return

    new_resource_state = handle_dynamic_subtask_state(
        log=mock_logger,
        resource_state=initial_state,
        fetch_changes=fetch_changes,
        fetch_page=fetch_page,
    )

    if expect_state_change:
        assert new_resource_state != initial_state
    else:
        assert new_resource_state == initial_state


@pytest.mark.parametrize(
    "initial_state, fetch_changes, fetch_page, new_subtask_id, expect_exception, expect_state_change",
    [
        (
            initial_state_with_subtasks(),
            fetch_changes_with_subtasks(),
            fetch_page_with_subtasks(),
            None,
            False,
            False,
        ),
        (
            initial_state_with_subtasks(),
            fetch_changes_with_subtasks(),
            None,
            None,
            False,
            False,
        ),
        (
            initial_state_with_subtasks(),
            None,
            fetch_page_with_subtasks(),
            None,
            False,
            False,
        ),
        (initial_state_with_subtasks(), None, None, None, False, False),
        (
            initial_state_with_subtasks(),
            fetch_changes_with_subtasks("extra_subtask"),
            fetch_page_with_subtasks("extra_subtask"),
            "extra_subtask",
            False,
            True,
        ),
    ],
)
def test_handle_dynamic_subtask_state_with_subtasks(
    initial_state,
    fetch_changes,
    fetch_page,
    new_subtask_id,
    expect_exception,
    expect_state_change,
    mock_logger,
):
    # Create a copy of the initial state so that we can compare it later
    # Python passes mutable objects by reference
    state_copy = deepcopy(initial_state)

    if expect_exception:
        with pytest.raises(ValueError):
            handle_dynamic_subtask_state(
                log=mock_logger,
                resource_state=state_copy,
                fetch_changes=fetch_changes,
                fetch_page=fetch_page,
            )
        return

    new_resource_state = handle_dynamic_subtask_state(
        log=mock_logger,
        resource_state=state_copy,
        fetch_changes=fetch_changes,
        fetch_page=fetch_page,
        initialize_subtask_state=initialize_subtask_state,
    )

    if expect_state_change:
        assert new_resource_state != initial_state
    else:
        assert new_resource_state == initial_state
