import json
import subprocess


def test_exempt_stream_state_migration(request):
    """
    Test that exempt streams (Accounts, Persons, etc.) correctly migrate from
    dict-based state (per-account subtasks) to non-dict state when the connector
    has capture_connected_accounts=true.

    This tests the fix for the merge_patch bug where old account keys would
    persist due to JSON Merge Patch (RFC 7396) recursive merging behavior.
    """
    # Simulate old dict-based state with multiple account keys, including a "dormant" account
    # that wouldn't be in the current connected accounts list.
    # The platform account ID from the test Stripe account.
    platform_account_id = "acct_platform123"

    old_state = {
        "bindingStateV1": {
            # Accounts is an exempt stream - should be migrated
            "Accounts": {
                "inc": {
                    platform_account_id: {"cursor": "2025-01-01T00:00:00+00:00"},
                    "acct_connected456": {"cursor": "2025-01-02T00:00:00+00:00"},
                    "acct_dormant789": {"cursor": "2025-01-03T00:00:00+00:00"},
                },
                "backfill": {
                    platform_account_id: None,
                    "acct_connected456": None,
                    "acct_dormant789": {"next_page": "page2", "cutoff": "2025-01-01T00:00:00+00:00"},
                },
                "last_initialized": "2025-01-01T00:00:00+00:00",
            },
        }
    }

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/state_migration.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "5s",
            "--timeout",
            "60s",
            "--initial-state",
            json.dumps(old_state),
            "--output-state",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # The connector should run successfully
    assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

    # Parse the output lines to find state checkpoints
    lines = result.stdout.strip().split("\n")
    state_updates = []
    for line in lines:
        if not line:
            continue
        try:
            parsed = json.loads(line)
            # State updates have format ["connectorState", {...}]
            if isinstance(parsed, list) and len(parsed) == 2 and parsed[0] == "connectorState":
                state_data = parsed[1]
                if isinstance(state_data, dict) and "updated" in state_data and state_data["updated"]:
                    state_updates.append(state_data["updated"])
        except json.JSONDecodeError:
            continue

    # We should have at least one state update from the migration
    assert len(state_updates) > 0, f"No state updates found. Output: {result.stdout}"

    # Check the final state - the Accounts binding should have non-dict state
    final_state = state_updates[-1]
    accounts_state = final_state.get("bindingStateV1", {}).get("Accounts", {})

    # The state should be migrated to non-dict format
    # inc should be a single Incremental object, not a dict of account IDs
    inc_state = accounts_state.get("inc", {})

    # Verify inc is NOT a dict with account keys (should have "cursor" directly)
    assert "cursor" in inc_state, (
        f"Expected inc to have 'cursor' key directly (non-dict format), "
        f"got: {inc_state}"
    )
    assert platform_account_id not in inc_state, (
        f"Platform account key should not exist in migrated state: {inc_state}"
    )
    assert "acct_connected456" not in inc_state, (
        f"Connected account key should not exist in migrated state: {inc_state}"
    )
    assert "acct_dormant789" not in inc_state, (
        f"Dormant account key should not exist in migrated state: {inc_state}"
    )

    # Verify backfill is also non-dict format or null (completed)
    backfill_state = accounts_state.get("backfill")
    if backfill_state is not None:
        # If backfill still exists, check it's non-dict format
        assert platform_account_id not in backfill_state, (
            f"Platform account key should not exist in backfill state: {backfill_state}"
        )
        assert "acct_connected456" not in backfill_state, (
            f"Connected account key should not exist in backfill state: {backfill_state}"
        )
        assert "acct_dormant789" not in backfill_state, (
            f"Dormant account key should not exist in backfill state: {backfill_state}"
        )


def test_non_exempt_stream_keeps_dict_state(request):
    """
    Test that non-exempt streams (like Customers) keep their dict-based state
    when capture_connected_accounts=true. Only exempt streams should be migrated.
    """
    platform_account_id = "acct_platform123"

    # For non-exempt streams, we need to provide state for all known connected accounts
    # plus the platform account. The connector will reconcile this state.
    old_state = {
        "bindingStateV1": {
            # Customers is NOT an exempt stream - should keep dict state
            "Customers": {
                "inc": {
                    platform_account_id: {"cursor": "2025-01-01T00:00:00+00:00"},
                    "acct_1PDu50Q1qnzwasyf": {"cursor": "2025-01-02T00:00:00+00:00"},
                    "acct_1PDugNPwuHD69DFD": {"cursor": "2025-01-02T00:00:00+00:00"},
                    "acct_1PEBIPKJ8Atczg1v": {"cursor": "2025-01-02T00:00:00+00:00"},
                },
                "backfill": {
                    platform_account_id: None,
                    "acct_1PDu50Q1qnzwasyf": None,
                    "acct_1PDugNPwuHD69DFD": None,
                    "acct_1PEBIPKJ8Atczg1v": None,
                },
                "last_initialized": "2025-01-01T00:00:00+00:00",
            },
        }
    }

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/state_migration_non_exempt.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "5s",
            "--timeout",
            "60s",
            "--initial-state",
            json.dumps(old_state),
            "--output-state",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

    lines = result.stdout.strip().split("\n")
    state_updates = []
    for line in lines:
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, list) and len(parsed) == 2 and parsed[0] == "connectorState":
                state_data = parsed[1]
                if isinstance(state_data, dict) and "updated" in state_data and state_data["updated"]:
                    state_updates.append(state_data["updated"])
        except json.JSONDecodeError:
            continue

    if len(state_updates) > 0:
        final_state = state_updates[-1]
        customers_state = final_state.get("bindingStateV1", {}).get("Customers", {})
        inc_state = customers_state.get("inc", {})

        # For non-exempt streams with connected accounts, the state should remain dict-based
        # It should have account keys, not a top-level "cursor" key
        # (unless the state was just initialized fresh, which also has account keys)
        if inc_state:
            # The state should have account keys for connected accounts
            # It should NOT have a top-level cursor (that would indicate non-dict format)
            has_account_keys = any(key.startswith("acct_") for key in inc_state.keys())
            has_direct_cursor = "cursor" in inc_state and not any(key.startswith("acct_") for key in inc_state.keys())

            assert has_account_keys or not has_direct_cursor, (
                f"Non-exempt stream should have dict-based state with account keys, "
                f"but got: {inc_state}"
            )
