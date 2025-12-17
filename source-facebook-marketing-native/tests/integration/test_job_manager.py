"""
Integration tests for job manager with mocked HTTP responses.

These tests validate the orchestration logic, splitting behavior, and error
handling using a MockHTTPSession that simulates Facebook's API behavior.
"""

import asyncio
import json
import pytest

from source_facebook_marketing_native.insights import FacebookInsightsJobManager
from source_facebook_marketing_native.insights.errors import CannotSplitFurtherError
from source_facebook_marketing_native.insights.manager import MAX_RETRIES

from tests.factories import (
    MockHTTPSession,
    create_job_submission_response,
    create_job_status_response,
    create_insights_response,
    create_discovery_response,
    create_data_limit_http_error,
)


# =============================================================================
# TestSuccessfulJob - Happy Path
# =============================================================================


class TestSuccessfulJob:
    """Test jobs that complete without splitting."""

    @pytest.mark.asyncio
    async def test_account_job_succeeds_first_try(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Account-level job succeeds without any splitting."""
        mock_http.add_response(create_job_submission_response("job_123"))
        mock_http.add_response(create_job_status_response("job_123", "Job Completed"))
        mock_http.add_response(
            create_insights_response(
                [
                    {"impressions": 100, "clicks": 10},
                    {"impressions": 200, "clicks": 20},
                ]
            )
        )

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        assert len(results) == 2
        assert results[0]["impressions"] == 100
        assert results[0]["account_id"] == "act_123"
        assert len(mock_http.requests) == 3  # submit, status, results

    @pytest.mark.asyncio
    async def test_handles_pagination(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Correctly handles paginated results."""
        mock_http.add_response(create_job_submission_response("job_456"))
        mock_http.add_response(create_job_status_response("job_456", "Job Completed"))
        mock_http.add_response(
            create_insights_response(
                [{"impressions": 100}],
                next_page="https://graph.facebook.com/v21.0/job_456/insights?cursor=abc",
            )
        )
        mock_http.add_response(
            create_insights_response([{"impressions": 200}, {"impressions": 300}])
        )

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        assert len(results) == 3
        assert len(mock_http.requests) == 4  # submit, status, page1, page2


# =============================================================================
# TestErrorHandling - Error Propagation and Retry Behavior
# =============================================================================


class TestErrorHandling:
    """Test error handling and propagation."""

    @pytest.mark.asyncio
    async def test_unknown_error_propagates(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Non-Facebook exceptions propagate without retry/split."""
        mock_http.add_response(RuntimeError("Network failure"))

        with pytest.raises(RuntimeError, match="Network failure"):
            async for _ in job_manager.fetch_insights(
                mock_log, mock_model, "act_123", time_range
            ):
                pass

    @pytest.mark.asyncio
    async def test_job_failed_status_triggers_retry_then_split(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Job Failed status retries MAX_RETRIES times, then splits."""
        job_counter = {"account_submits": 0, "campaign_submits": 0}

        def handle_insights_post(request):
            params = request.get("params", {})
            if params and "filtering" in params:
                job_counter["campaign_submits"] += 1
                return create_job_submission_response(
                    f"campaign_job_{job_counter['campaign_submits']}"
                )
            job_counter["account_submits"] += 1
            return create_job_submission_response(
                f"account_job_{job_counter['account_submits']}"
            )

        def handle_insights_get(request):
            return create_discovery_response(["c1"], "campaign_id")

        def handle_status(request):
            job_id = request["url"].split("/")[-1]
            if job_id.startswith("account_job"):
                return create_job_status_response(job_id, "Job Failed", percent=0)
            return create_job_status_response(job_id, "Job Completed")

        def handle_results(request):
            return create_insights_response([{"impressions": 100}])

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_insights_post, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_insights_get, method="GET")
        mock_http.add_handler(r"/(account_job|campaign_job)[^/]*$", handle_status, method="GET")
        mock_http.add_handler(r"/insights$", handle_results, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        assert job_counter["account_submits"] == MAX_RETRIES
        assert job_counter["campaign_submits"] >= 1
        assert len(results) >= 1


# =============================================================================
# TestDataLimitError - Immediate Split Without Retries
# =============================================================================


class TestDataLimitError:
    """Test immediate split on data limit errors."""

    @pytest.mark.asyncio
    async def test_data_limit_triggers_immediate_split_without_retries(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Data limit error (code 100, subcode 1487534) splits immediately - no retries."""
        call_count = {"account_submits": 0, "campaign_submits": 0}

        def handle_submit(request):
            params = request.get("params", {})
            if params and "filtering" in params:
                call_count["campaign_submits"] += 1
                return create_job_submission_response(f"campaign_job_{call_count['campaign_submits']}")
            call_count["account_submits"] += 1
            return create_job_submission_response(f"account_job_{call_count['account_submits']}")

        def handle_status(request):
            job_id = request["url"].split("/")[-1]
            return create_job_status_response(job_id, "Job Completed")

        def handle_results(request):
            job_id = request["url"].split("/")[-2]
            if job_id.startswith("account_job"):
                raise create_data_limit_http_error()
            return create_insights_response([{"impressions": 50}])

        def handle_discovery(request):
            return create_discovery_response(["c1", "c2"], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"_job_\d+$", handle_status, method="GET")
        mock_http.add_handler(r"_job_\d+/insights", handle_results, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        # Key assertion: account job was only submitted ONCE (no retries)
        assert call_count["account_submits"] == 1, "Data limit should not retry"
        assert call_count["campaign_submits"] >= 1, "Should have split to campaigns"
        assert len(results) >= 1


# =============================================================================
# TestJobSplitting - Splitting Behavior
# =============================================================================


class TestJobSplitting:
    """Test splitting behavior on job failures."""

    @pytest.mark.asyncio
    async def test_verifies_filtering_params_on_split(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Verify split jobs include correct filtering parameters."""
        submitted_requests = []

        def handle_submit(request):
            submitted_requests.append(request)
            params = request.get("params", {})
            if params and "filtering" in params:
                return create_job_submission_response("campaign_job_1")
            return create_job_submission_response("account_job_1")

        def handle_status(request):
            job_id = request["url"].split("/")[-1]
            if job_id == "account_job_1":
                return create_job_status_response(job_id, "Job Failed")
            return create_job_status_response(job_id, "Job Completed")

        def handle_results(request):
            return create_insights_response([{"impressions": 100}])

        def handle_discovery(request):
            return create_discovery_response(["campaign_1", "campaign_2"], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"_job_\d+/insights$", handle_results, method="GET")
        mock_http.add_handler(r"_job_\d+$", handle_status, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        campaign_submissions = [
            r for r in submitted_requests
            if r.get("params") and "filtering" in r["params"]
        ]

        assert len(campaign_submissions) >= 1
        for sub in campaign_submissions:
            filtering = json.loads(sub["params"]["filtering"])
            assert filtering[0]["field"] == "campaign.id"
            assert filtering[0]["operator"] == "IN"

    @pytest.mark.asyncio
    async def test_binary_split_at_campaign_level(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """When account fails and discovers 4 campaigns, binary splits to 2x2."""
        submitted_filters = []
        job_counter = {"value": 0}

        def handle_submit(request):
            job_counter["value"] += 1
            params = request.get("params", {})
            if params and "filtering" in params:
                filtering = json.loads(params["filtering"])
                submitted_filters.append(filtering[0])
                return create_job_submission_response(f"campaign_job_{job_counter['value']}")
            return create_job_submission_response(f"account_job_{job_counter['value']}")

        def handle_status(request):
            job_id = request["url"].split("/")[-1]
            if job_id.startswith("account_job"):
                return create_job_status_response(job_id, "Job Failed")
            # All campaign jobs succeed
            return create_job_status_response(job_id, "Job Completed")

        def handle_results(request):
            return create_insights_response([{"impressions": 100}])

        def handle_discovery(request):
            return create_discovery_response(["c1", "c2", "c3", "c4"], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"_job_\d+/insights$", handle_results, method="GET")
        mock_http.add_handler(r"_job_\d+$", handle_status, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        # Account fails, discovers 4 campaigns, immediately binary splits to 2x2
        # (no 4-campaign job is ever submitted - split happens at discovery time)
        two_campaign_filters = [f for f in submitted_filters if len(f["value"]) == 2]
        assert len(two_campaign_filters) == 2, "Should split 4 campaigns into 2 jobs of 2"

        # Verify the split is correct: [c1,c2] and [c3,c4]
        all_campaign_ids = set()
        for f in two_campaign_filters:
            all_campaign_ids.update(f["value"])
        assert all_campaign_ids == {"c1", "c2", "c3", "c4"}, "All campaigns should be covered"

        # Should have results from both split jobs
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_full_hierarchy_descent(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Test full descent: Account -> Campaign -> AdSet -> Ad (single entities)."""
        discovery_calls = []

        def handle_submit(request):
            return create_job_submission_response(f"job_{len(discovery_calls)}")

        def handle_status(request):
            # All jobs fail to force descent
            job_id = request["url"].split("/")[-1]
            return create_job_status_response(job_id, "Job Failed")

        def handle_discovery(request):
            params = request.get("params", {})
            if params and "filtering" in params:
                filtering = json.loads(params["filtering"])
                field = filtering[0]["field"]
                discovery_calls.append(field)
                if field == "campaign.id":
                    # Single campaign -> descend to adsets
                    return create_discovery_response(["single_adset"], "adset_id")
                elif field == "adset.id":
                    # Single adset -> descend to ads
                    return create_discovery_response(["single_ad"], "ad_id")
            # Account level -> single campaign
            discovery_calls.append("account")
            return create_discovery_response(["single_campaign"], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"/job_", handle_status, method="GET")

        with pytest.raises(CannotSplitFurtherError):
            async for _ in job_manager.fetch_insights(
                mock_log, mock_model, "act_123", time_range
            ):
                pass

        # Verify we descended through the full hierarchy
        # Note: ad.id level doesn't call discovery - it raises CannotSplitFurtherError directly
        assert "account" in discovery_calls, "Should discover campaigns at account level"
        assert "campaign.id" in discovery_calls, "Should discover adsets for campaign"
        assert "adset.id" in discovery_calls, "Should discover ads for adset"


# =============================================================================
# TestAtomicFailure - Terminal Failure Cases
# =============================================================================


class TestAtomicFailure:
    """Test behavior when splitting is no longer possible."""

    @pytest.mark.asyncio
    async def test_single_ad_failure_raises_exception(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Single ad job that fails raises CannotSplitFurtherError."""

        def handle_submit(request):
            return create_job_submission_response("job_1")

        def handle_status(request):
            return create_job_status_response("job_1", "Job Failed")

        def handle_discovery(request):
            params = request.get("params", {})
            if params and "filtering" in params:
                filtering = json.loads(params["filtering"])
                field = filtering[0]["field"]
                if field == "campaign.id":
                    return create_discovery_response(["single_adset"], "adset_id")
                elif field == "adset.id":
                    return create_discovery_response(["single_ad"], "ad_id")
            return create_discovery_response(["single_campaign"], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"/job_", handle_status, method="GET")

        with pytest.raises(CannotSplitFurtherError):
            async for _ in job_manager.fetch_insights(
                mock_log, mock_model, "act_123", time_range
            ):
                pass

    @pytest.mark.asyncio
    async def test_empty_discovery_results(
        self, job_manager, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Account job fails, campaign discovery returns empty list - completes gracefully."""

        def handle_submit(request):
            return create_job_submission_response("job_1")

        def handle_status(request):
            return create_job_status_response("job_1", "Job Failed")

        def handle_discovery(request):
            return create_discovery_response([], "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"/job_", handle_status, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        assert len(results) == 0


# =============================================================================
# TestConcurrency - Semaphore and Multi-Account
# =============================================================================


class TestConcurrency:
    """Test concurrent job execution and account isolation."""

    @pytest.mark.asyncio
    async def test_respects_semaphore_limit(
        self, mock_http, mock_log, mock_model, time_range, mock_sleep
    ):
        """Verify concurrent jobs don't exceed semaphore limit."""
        test_max_concurrent = 5
        job_manager = FacebookInsightsJobManager(
            http=mock_http,
            base_url="https://graph.facebook.com/v21.0",
            log=mock_log,
            account_id="test_account",
        )
        job_manager._semaphore = asyncio.Semaphore(test_max_concurrent)

        job_counter = {"submissions": 0}
        many_campaigns = [f"c{i}" for i in range(20)]

        def handle_submit(request):
            job_counter["submissions"] += 1
            params = request.get("params", {})
            if params and "filtering" in params:
                return create_job_submission_response(f"campaign_job_{job_counter['submissions']}")
            return create_job_submission_response("account_job_1")

        def handle_status(request):
            job_id = request["url"].split("/")[-1]
            if job_id == "account_job_1":
                return create_job_status_response(job_id, "Job Failed")
            return create_job_status_response(job_id, "Job Completed")

        def handle_results(request):
            return create_insights_response([{"impressions": 1}])

        def handle_discovery(request):
            return create_discovery_response(many_campaigns, "campaign_id")

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")
        mock_http.add_handler(r"/act_[^/]+/insights$", handle_discovery, method="GET")
        mock_http.add_handler(r"_job_\d+/insights$", handle_results, method="GET")
        mock_http.add_handler(r"/account_job_\d+$", handle_status, method="GET")
        mock_http.add_handler(r"/campaign_job_\d+$", handle_status, method="GET")

        results = []
        async for record in job_manager.fetch_insights(
            mock_log, mock_model, "act_123", time_range
        ):
            results.append(record)

        # Factor of 3 accounts for concurrent operations per job: submission, status
        # polling, and result fetching can overlap across different jobs in flight
        assert mock_http.max_concurrent_seen <= test_max_concurrent * 3
        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_multiple_accounts_independent_execution(
        self, mock_log, mock_model, time_range, mock_sleep
    ):
        """Two accounts with separate job managers execute independently."""
        http_1 = MockHTTPSession()
        http_2 = MockHTTPSession()

        jm_1 = FacebookInsightsJobManager(
            http=http_1,
            base_url="https://graph.facebook.com/v21.0",
            log=mock_log,
            account_id="act_111",
        )
        jm_2 = FacebookInsightsJobManager(
            http=http_2,
            base_url="https://graph.facebook.com/v21.0",
            log=mock_log,
            account_id="act_222",
        )

        http_1.add_response(create_job_submission_response("job_1"))
        http_1.add_response(create_job_status_response("job_1", "Job Completed"))
        http_1.add_response(create_insights_response([{"impressions": 1000}]))

        http_2.add_response(create_job_submission_response("job_2"))
        http_2.add_response(create_job_status_response("job_2", "Job Completed"))
        http_2.add_response(create_insights_response([{"impressions": 2000}]))

        async def fetch(jm, account_id):
            results = []
            async for record in jm.fetch_insights(mock_log, mock_model, account_id, time_range):
                results.append(record)
            return results

        r1, r2 = await asyncio.gather(fetch(jm_1, "act_111"), fetch(jm_2, "act_222"))

        assert r1[0]["impressions"] == 1000
        assert r1[0]["account_id"] == "act_111"
        assert r2[0]["impressions"] == 2000
        assert r2[0]["account_id"] == "act_222"
