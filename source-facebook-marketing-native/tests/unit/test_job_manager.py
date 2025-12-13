"""
Unit tests for job splitting logic.

Tests the core splitting decisions: when jobs can split, how binary splitting
divides entity lists, and error parsing that triggers splits.
"""

import logging

import pytest

from estuary_cdk.http import HTTPError

from source_facebook_marketing_native.client import FacebookAPIError
from source_facebook_marketing_native.insights import FacebookInsightsJobManager
from source_facebook_marketing_native.insights.types import (
    JobScope,
    InsightsJob,
)
from source_facebook_marketing_native.insights.errors import (
    DataLimitExceededError,
    try_parse_facebook_api_error,
)


class TestInsightsJobCanSplit:
    """Tests for InsightsJob.can_split() - the core splitting decision logic."""

    def test_account_job_can_always_split(self):
        """Account-level jobs can always split (discover campaigns)."""
        job = InsightsJob(scope=JobScope.ACCOUNT)
        assert job.can_split() is True

    def test_single_campaign_can_split(self):
        """Single campaign can descend to adsets."""
        job = InsightsJob(scope=JobScope.CAMPAIGNS, entity_ids=["1"])
        assert job.can_split() is True

    def test_single_adset_can_split(self):
        """Single adset can descend to ads."""
        job = InsightsJob(scope=JobScope.ADSETS, entity_ids=["1"])
        assert job.can_split() is True

    def test_multiple_ads_can_split(self):
        """Multiple ads can binary split."""
        job = InsightsJob(scope=JobScope.ADS, entity_ids=["1", "2"])
        assert job.can_split() is True

    def test_single_ad_cannot_split(self):
        """Single ad is atomic - the terminal condition for splitting."""
        job = InsightsJob(scope=JobScope.ADS, entity_ids=["1"])
        assert job.can_split() is False

    def test_ads_with_empty_entity_ids_cannot_split(self):
        """ADS scope with empty entity_ids cannot split."""
        job = InsightsJob(scope=JobScope.ADS, entity_ids=[])
        assert job.can_split() is False


class TestBinarySplit:
    """Tests for _binary_split - how entity lists are divided."""

    @pytest.fixture
    def job_manager(self) -> FacebookInsightsJobManager:
        """Create a minimal job manager for testing _binary_split."""
        return FacebookInsightsJobManager(
            http=None,  # type: ignore
            base_url="https://graph.facebook.com/v21.0",
            log=logging.getLogger("test"),
            account_id="test_account",
        )

    def test_empty_list_returns_empty(self, job_manager: FacebookInsightsJobManager):
        """Empty entity list returns empty job list."""
        result = job_manager._binary_split([], JobScope.CAMPAIGNS, depth=1, parent_scope=JobScope.ACCOUNT)
        assert result == []

    def test_single_entity_returns_single_job(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Single entity returns one job (no split possible)."""
        result = job_manager._binary_split(["1"], JobScope.CAMPAIGNS, depth=1, parent_scope=JobScope.ACCOUNT)
        assert len(result) == 1
        assert result[0].entity_ids == ["1"]
        assert result[0].depth == 1
        assert result[0].parent_scope == JobScope.ACCOUNT

    def test_two_entities_split_evenly(self, job_manager: FacebookInsightsJobManager):
        """Two entities split into two jobs with one each."""
        result = job_manager._binary_split(["1", "2"], JobScope.CAMPAIGNS, depth=1, parent_scope=JobScope.ACCOUNT)
        assert len(result) == 2
        assert result[0].entity_ids == ["1"]
        assert result[1].entity_ids == ["2"]
        # Verify depth and parent_scope are propagated
        for job in result:
            assert job.depth == 1
            assert job.parent_scope == JobScope.ACCOUNT

    def test_odd_number_split(self, job_manager: FacebookInsightsJobManager):
        """Odd counts: first half gets floor(n/2), second gets ceil(n/2)."""
        result = job_manager._binary_split(
            ["a", "b", "c", "d", "e"], JobScope.CAMPAIGNS, depth=2, parent_scope=JobScope.CAMPAIGNS
        )
        assert len(result) == 2
        assert result[0].entity_ids == ["a", "b"]
        assert result[1].entity_ids == ["c", "d", "e"]

    def test_large_list_splits_evenly(self, job_manager: FacebookInsightsJobManager):
        """Large list splits into two equal halves."""
        ids = [str(i) for i in range(1000)]
        result = job_manager._binary_split(ids, JobScope.ADS, depth=3, parent_scope=JobScope.ADSETS)
        assert len(result) == 2
        assert len(result[0].entity_ids or []) == 500
        assert len(result[1].entity_ids or []) == 500
        # Verify depth tracking on large splits
        assert result[0].depth == 3
        assert result[1].depth == 3


class TestTryParseFacebookApiError:
    """Tests for error parsing that determines split behavior."""

    def test_returns_data_limit_exceeded_for_code_100_subcode_1487534(self):
        """Data limit error (code 100, subcode 1487534) triggers immediate split."""
        error = HTTPError(
            "Encountered HTTP error status 400 which cannot be retried.\n"
            "URL: https://graph.facebook.com/v21.0/act_123/insights\n"
            "Response:\n"
            '{"error": {"message": "Please reduce the amount of data...", '
            '"type": "OAuthException", "code": 100, "error_subcode": 1487534}}',
            400,
        )
        result = try_parse_facebook_api_error(error)
        assert isinstance(result, DataLimitExceededError)

    def test_returns_facebook_api_error_for_other_errors(self):
        """Other Facebook errors are parsed but don't trigger immediate split."""
        error = HTTPError(
            "Response:\n"
            '{"error": {"message": "Invalid token", "type": "OAuthException", '
            '"code": 190, "error_subcode": 463}}',
            400,
        )
        result = try_parse_facebook_api_error(error)
        assert isinstance(result, FacebookAPIError)
        assert result.error.code == 190

    def test_returns_none_for_non_facebook_errors(self):
        """Non-Facebook error formats return None (no special handling)."""
        # Missing Response: section
        assert try_parse_facebook_api_error(HTTPError("Network error", 500)) is None
        # Malformed JSON
        assert try_parse_facebook_api_error(HTTPError("Response:\nnot json", 400)) is None
        # Missing error key
        assert try_parse_facebook_api_error(HTTPError('Response:\n{"data": []}', 400)) is None
