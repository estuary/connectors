# Facebook Marketing Native Connector Tests

This directory contains tests for the `source-facebook-marketing-native` connector, organized into unit, integration, and end-to-end (e2e) tests.

## Directory Structure

```
tests/
├── README.md              # This file
├── conftest.py            # Shared pytest fixtures
├── factories/
│   ├── __init__.py        # Exports MockHTTPSession and response factories
│   ├── http.py            # MockHTTPSession class
│   └── responses.py       # Response factory functions
├── unit/
│   └── test_job_manager.py    # Unit tests for job splitting logic
├── integration/
│   └── test_job_manager.py    # Integration tests with mocked HTTP
└── e2e/
    ├── test_snapshots.py      # End-to-end tests using flowctl
    └── snapshots/             # Snapshot files for e2e tests
```

## Running Tests

```bash
# Run all tests
poetry run pytest tests/ -v

# Run specific test category
poetry run pytest tests/unit/ -v
poetry run pytest tests/integration/ -v
poetry run pytest tests/e2e/ -v

# Run with DEBUG logging (see job manager logs)
poetry run pytest tests/integration/ -v --log-cli-level=DEBUG

# Run specific test
poetry run pytest tests/integration/test_job_manager.py::TestJobSplitting::test_binary_split_at_campaign_level -v

# Run with coverage
poetry run pytest tests/ --cov=source_facebook_marketing_native.insights
```

## Test Categories

### Unit Tests (`tests/unit/`)

Fast, isolated tests for pure functions and data structures. No mocking of external dependencies.

| Test Class | Description |
|------------|-------------|
| `TestInsightsJobCanSplit` | Tests `InsightsJob.can_split()` decision logic |
| `TestBinarySplit` | Tests `_binary_split()` algorithm for dividing entity lists |
| `TestTryParseFacebookApiError` | Tests error parsing that triggers job splitting |

### Integration Tests (`tests/integration/`)

Tests that verify the `FacebookInsightsJobManager` orchestration using mocked HTTP responses. No network access required.

| Test Class | Description |
|------------|-------------|
| `TestSuccessfulJob` | Happy path - jobs complete without splitting |
| `TestErrorHandling` | Error propagation and retry behavior |
| `TestDataLimitError` | Immediate split on data limit errors (no retries) |
| `TestJobSplitting` | Core splitting logic with filtering verification |
| `TestAtomicFailure` | Edge cases - single ad failure, empty discovery |
| `TestConcurrency` | Semaphore limits and multi-account isolation |

### E2E Tests (`tests/e2e/`)

End-to-end tests that run the connector via `flowctl` with real (encrypted) credentials.

| Test | Description |
|------|-------------|
| `test_spec` | Validates connector spec output |
| `test_discover` | Validates connector discovery output |

## Test Infrastructure

### MockHTTPSession (`factories/http.py`)

A mock HTTP session that replaces `estuary_cdk.http.HTTPSession` in tests. Features:

- **URL-Pattern Routing**: Register handlers with regex patterns
- **Method Filtering**: Handlers can be specific to GET/POST
- **Concurrency Tracking**: Tracks `max_concurrent_seen` for semaphore tests
- **FIFO Fallback**: Queue responses for simple sequential tests

```python
from tests.factories import MockHTTPSession

mock_http = MockHTTPSession()

# Pattern-based handler (recommended for complex tests)
def handle_submit(request):
    return {"report_run_id": "job_123"}

mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")

# FIFO queue (for simple sequential tests)
mock_http.add_response({"report_run_id": "job_123"})
mock_http.add_response({"id": "job_123", "async_status": "Job Completed"})
```

**Important**: The real `HTTPSession.request()` returns `bytes`, and `MockHTTPSession` handles this automatically by JSON-encoding dict responses.

### Response Factories (`factories/responses.py`)

Factory functions that create properly-formatted Facebook API responses:

```python
from tests.factories import (
    create_job_submission_response,
    create_job_status_response,
    create_insights_response,
    create_discovery_response,
    create_data_limit_http_error,
    create_facebook_api_http_error,
)

# Job submission response
create_job_submission_response("job_123")
# Returns: {"report_run_id": "job_123"}

# Job status response
create_job_status_response("job_123", "Job Completed", percent=100)
# Returns: {"id": "job_123", "async_status": "Job Completed", "async_percent_completion": 100}

# Insights data response
create_insights_response(
    [{"impressions": 100, "clicks": 10}],
    next_page="https://graph.facebook.com/v21.0/job_123/insights?cursor=abc"
)
# Returns: {"data": [...], "paging": {"next": "..."}}

# Entity discovery response (for splitting)
create_discovery_response(["c1", "c2", "c3"], "campaign_id")
# Returns: {"data": [{"campaign_id": "c1"}, {"campaign_id": "c2"}, ...]}

# Data limit error (triggers immediate split)
create_data_limit_http_error()
# Returns: HTTPError with code=100, subcode=1487534

# Generic Facebook API error
create_facebook_api_http_error(code=190, message="Invalid token")
```

### Shared Fixtures (`conftest.py`)

Pytest fixtures available to all tests:

| Fixture | Description |
|---------|-------------|
| `mock_http` | Fresh `MockHTTPSession` instance |
| `mock_log` | Logger that outputs when running with `-v` |
| `job_manager` | Pre-configured `FacebookInsightsJobManager` |
| `mock_model` | Mock insights model with required attributes |
| `time_range` | Standard time range `{"since": "2024-01-01", "until": "2024-01-01"}` |
| `mock_sleep` | Patches `asyncio.sleep` to be instant |

## Key Behaviors Tested

### Job Lifecycle

1. **Submit**: POST to `/act_{account}/insights` returns `report_run_id`
2. **Poll**: GET to `/{job_id}` returns status and completion percentage
3. **Fetch**: GET to `/{job_id}/insights` returns paginated results

### Facebook Async Job Statuses

| Status | Behavior |
|--------|----------|
| Job Not Started | Continue polling |
| Job Started | Continue polling |
| Job Running | Continue polling (with percentage) |
| Job Completed | Fetch results |
| Job Failed | Retry up to MAX_RETRIES, then split |
| Job Skipped | Same as Job Failed (job expired) |

### Job Splitting Hierarchy

When a job fails after MAX_RETRIES (3), it splits to a finer granularity:

```
ACCOUNT (all data)
    ↓ fails → discover campaigns
CAMPAIGNS (filtered by campaign.id)
    ↓ fails → binary split OR descend to adsets
ADSETS (filtered by adset.id)
    ↓ fails → binary split OR descend to ads
ADS (filtered by ad.id)
    ↓ fails → binary split OR CannotSplitFurtherError
```

**Binary Split**: When multiple entities exist, split the list in half:
- 4 campaigns → 2 jobs with 2 campaigns each
- 3 campaigns → 2 jobs with 1 and 2 campaigns

**Hierarchy Descent**: When a single entity fails, discover child entities:
- 1 campaign fails → discover its adsets
- 1 adset fails → discover its ads
- 1 ad fails → `CannotSplitFurtherError`

### Data Limit Error

Facebook returns error code 100 with subcode 1487534 when query scope is too broad. This triggers **immediate split without retries**.

```python
# This error format triggers immediate split
{
    "error": {
        "message": "Please reduce the amount of data you're asking for",
        "type": "OAuthException",
        "code": 100,
        "error_subcode": 1487534
    }
}
```

## Writing New Tests

### Integration Test Pattern

```python
@pytest.mark.asyncio
async def test_example(self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep):
    """Describe what this test verifies."""

    # 1. Track state if needed
    call_count = {"submits": 0}

    # 2. Define handlers
    def handle_submit(request):
        call_count["submits"] += 1
        params = request.get("params", {})
        if params and "filtering" in params:
            # Campaign-level job (has filtering)
            return create_job_submission_response("campaign_job_1")
        # Account-level job (no filtering)
        return create_job_submission_response("account_job_1")

    def handle_status(request):
        job_id = request["url"].split("/")[-1]
        if job_id.startswith("account"):
            return create_job_status_response(job_id, "Job Failed")
        return create_job_status_response(job_id, "Job Completed")

    def handle_results(request):
        return create_insights_response([{"impressions": 100}])

    def handle_discovery(request):
        return create_discovery_response(["c1", "c2"], "campaign_id")

    # 3. Register handlers (ORDER MATTERS - more specific patterns first)
    mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
    mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
    mock_http.add_handler(r"_job_\d+/insights$", handle_results, method="GET")  # Before status!
    mock_http.add_handler(r"_job_\d+$", handle_status, method="GET")

    # 4. Execute
    results = []
    async for record in job_manager.fetch_insights(
        mock_log, mock_model, "act_123", time_range
    ):
        results.append(record)

    # 5. Assert
    assert len(results) >= 1
    assert call_count["submits"] >= 1
```

### URL Pattern Gotchas

**Handler order matters!** Register more specific patterns first:

```python
# CORRECT ORDER
mock_http.add_handler(r"_job_\d+/insights$", handle_results)  # More specific
mock_http.add_handler(r"_job_\d+$", handle_status)            # Less specific

# WRONG - status handler would match both URLs
mock_http.add_handler(r"_job_\d+", handle_status)   # Matches /job_1 AND /job_1/insights
mock_http.add_handler(r"_job_\d+/insights$", handle_results)  # Never reached
```

**Use `$` anchors** to prevent partial matches:
- `/job_\d+$` - matches `/job_123` only
- `/job_\d+/insights$` - matches `/job_123/insights` only

### Distinguishing Submit vs Discovery

Both use the same URL pattern (`/act_{account}/insights`) but different methods:

```python
mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")     # Job submission
mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")   # Entity discovery
```

## Common Assertions

```python
# Verify retry count
assert job_counter["account_submits"] == MAX_RETRIES  # Should retry 3 times

# Verify split occurred
assert job_counter["campaign_submits"] >= 1  # Campaign jobs were created

# Verify filtering params
campaign_submissions = [r for r in mock_http.requests if "filtering" in str(r.get("params", {}))]
assert len(campaign_submissions) == 2

# Verify no retries (for data limit)
assert job_counter["submits"] == 1  # Only ONE submission before split

# Verify concurrency bounded
assert mock_http.max_concurrent_seen <= test_max_concurrent

# Verify account_id added to results
assert all(r["account_id"] == "act_123" for r in results)
```

## Debugging Tips

1. **Run with DEBUG logging** to see job manager decisions:
   ```bash
   poetry run pytest tests/integration/test_job_manager.py -v --log-cli-level=DEBUG
   ```

2. **Print mock requests** to see what URLs were called:
   ```python
   for r in mock_http.requests:
       print(f"{r['method']} {r['url']} params={r.get('params')}")
   ```

3. **Check handler matching** - if responses aren't being returned, verify:
   - Pattern matches the URL (test with `re.search(pattern, url)`)
   - Method filter matches (GET vs POST)
   - Handler order (specific before general)

4. **Async test hanging?** Check:
   - `mock_sleep` fixture is included
   - All expected responses are queued/handled
   - No infinite polling loop (job status never reaches terminal state)
