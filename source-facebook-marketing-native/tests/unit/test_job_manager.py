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
from source_facebook_marketing_native.insights.errors import CannotSplitFurtherError
from source_facebook_marketing_native.models import (
    AdsInsights,
    AdsInsightsActionType,
    AdsInsightsAgeAndGender,
    AdsInsightsComscoreMarket,
    AdsInsightsCountry,
    AdsInsightsPlatformAndDevice,
    AdsInsightsRegion,
    InsightsConfig,
    build_custom_ads_insights_model,
)
from source_facebook_marketing_native.insights.types import (
    FieldSplitPart,
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


class TestFieldSplitJoinKey:
    """Tests for the two key lists field splitting needs to keep distinct."""

    @pytest.fixture
    def job_manager(self) -> FacebookInsightsJobManager:
        return FacebookInsightsJobManager(
            http=None,  # type: ignore
            base_url="https://graph.facebook.com/v21.0",
            log=logging.getLogger("test"),
            account_id="test_account",
        )

    def test_plain_stream_requests_all_its_keys(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Every primary key of ads_insights is a requestable field."""
        assert job_manager._requested_key_fields(AdsInsights) == [
            "account_id",
            "ad_id",
            "date_start",
        ]
        assert job_manager._join_key(AdsInsights) == [
            "account_id",
            "ad_id",
            "date_start",
        ]

    def test_breakdown_columns_are_keys_but_not_requested(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Breakdown columns key the row but must not be added to `fields`.

        Facebook returns them because the `breakdowns` parameter is unchanged
        across parts; asking for them as fields would be rejected. They must
        still join the parts back together, or every demographic row of an
        ad-day collapses into one record.
        """
        requested = job_manager._requested_key_fields(AdsInsightsAgeAndGender)
        assert requested == ["account_id", "ad_id", "date_start"]

        assert job_manager._join_key(AdsInsightsAgeAndGender) == [
            "account_id",
            "ad_id",
            "age",
            "date_start",
            "gender",
        ]

    def test_every_breakdown_stream_joins_on_its_whole_key(
        self, job_manager: FacebookInsightsJobManager
    ):
        """No breakdown stream may lose part of its key to the requestable filter."""
        for model in [
            AdsInsights,
            AdsInsightsActionType,
            AdsInsightsAgeAndGender,
            AdsInsightsComscoreMarket,
            AdsInsightsCountry,
            AdsInsightsPlatformAndDevice,
            AdsInsightsRegion,
        ]:
            pointers = [key.lstrip("/") for key in model.primary_keys]
            assert job_manager._join_key(model) == pointers, model.name

    def test_custom_model_keys_on_date_start_it_never_requests(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Custom insights models key on `/date_start` without listing it.

        `level_specific_fields()` contributes only the account/campaign/adset/ad
        id and name columns, so `date_start` is in neither `fields` nor
        `breakdowns`. Facebook returns it regardless, because `time_increment=1`
        is always sent - which is why obtainability cannot be decided from the
        model alone.
        """
        model = build_custom_ads_insights_model(
            InsightsConfig(
                name="my_custom",
                level="ad",
                fields="impressions,clicks",
                breakdowns="age,gender",
            )
        )
        assert "date_start" not in model.fields
        assert "date_start" not in model.breakdowns

        assert job_manager._join_key(model) == [
            "account_id",
            "ad_id",
            "age",
            "date_start",
            "gender",
        ]


class TestVerifyJoinKey:
    """Tests for _verify_join_key - the chunk-level guard before merging."""

    @pytest.fixture
    def job_manager(self) -> FacebookInsightsJobManager:
        return FacebookInsightsJobManager(
            http=None,  # type: ignore
            base_url="https://graph.facebook.com/v21.0",
            log=logging.getLogger("test"),
            account_id="test_account",
        )

    def test_rows_carrying_the_whole_key_pass(
        self, job_manager: FacebookInsightsJobManager
    ):
        records = [
            {"ad_id": "a1", "date_start": "2024-01-01", "age": "25-34", "clicks": 1},
            {"ad_id": "a1", "date_start": "2024-01-01", "age": "35-44", "clicks": 2},
        ]
        job_manager._verify_join_key(
            records, ["ad_id", "age", "date_start"], ["clicks"], "job"
        )

    def test_missing_key_column_is_refused(
        self, job_manager: FacebookInsightsJobManager
    ):
        """A row without a key column cannot be told apart from its siblings."""
        records = [
            {"ad_id": "a1", "date_start": "2024-01-01", "clicks": 1},
            {"ad_id": "a1", "date_start": "2024-01-01", "clicks": 2},
        ]

        with pytest.raises(CannotSplitFurtherError, match=r"\['age'\]"):
            job_manager._verify_join_key(
                records, ["ad_id", "age", "date_start"], ["clicks"], "job"
            )

    def test_every_missing_column_is_named(
        self, job_manager: FacebookInsightsJobManager
    ):
        """The error names all of them, not just the first row's."""
        records = [
            {"ad_id": "a1", "date_start": "2024-01-01", "age": "25-34"},
            {"ad_id": "a1", "date_start": "2024-01-01", "gender": "male"},
        ]

        with pytest.raises(CannotSplitFurtherError, match=r"\['age', 'gender'\]"):
            job_manager._verify_join_key(
                records, ["ad_id", "age", "date_start", "gender"], ["clicks"], "job"
            )

    def test_a_chunk_with_no_rows_has_nothing_to_verify(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Emptiness is not a key violation; it is handled as degradation."""
        job_manager._verify_join_key([], ["ad_id", "age"], ["clicks"], "job")


class TestMergeByPrimaryKey:
    """Tests for _merge_by_primary_key - re-joining field-split parts."""

    @pytest.fixture
    def job_manager(self) -> FacebookInsightsJobManager:
        return FacebookInsightsJobManager(
            http=None,  # type: ignore
            base_url="https://graph.facebook.com/v21.0",
            log=logging.getLogger("test"),
            account_id="test_account",
        )

    def test_disjoint_columns_merge_into_one_record(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Parts hold different columns for the same row."""
        parts = [
            FieldSplitPart(
                fields=["impressions"],
                records=[{"ad_id": "a1", "date_start": "2024-01-01", "impressions": 100}],
            ),
            FieldSplitPart(
                fields=["clicks"],
                records=[{"ad_id": "a1", "date_start": "2024-01-01", "clicks": 5}],
            ),
        ]
        merged = job_manager._merge_by_primary_key(parts, ["ad_id", "date_start"])

        assert merged == [
            {
                "ad_id": "a1",
                "date_start": "2024-01-01",
                "impressions": 100,
                "clicks": 5,
            }
        ]

    def test_distinct_rows_stay_distinct(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Different primary keys must not collapse together."""
        parts = [
            FieldSplitPart(
                fields=["impressions"],
                records=[
                    {"ad_id": "a1", "date_start": "2024-01-01", "impressions": 1},
                    {"ad_id": "a2", "date_start": "2024-01-01", "impressions": 2},
                ],
            ),
            FieldSplitPart(
                fields=["clicks"],
                records=[
                    {"ad_id": "a1", "date_start": "2024-01-01", "clicks": 10},
                    {"ad_id": "a2", "date_start": "2024-01-01", "clicks": 20},
                ],
            ),
        ]
        merged = job_manager._merge_by_primary_key(parts, ["ad_id", "date_start"])

        assert len(merged) == 2
        by_ad = {record["ad_id"]: record for record in merged}
        assert by_ad["a1"]["impressions"] == 1 and by_ad["a1"]["clicks"] == 10
        assert by_ad["a2"]["impressions"] == 2 and by_ad["a2"]["clicks"] == 20

    def test_row_missing_from_a_part_still_yields_a_record(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Merging over the union, not the intersection.

        Facebook returns primary-key-only rows rather than omitting rows whose
        metrics are all null, so parts normally align. If one ever doesn't, the
        row should still surface with whatever was obtained instead of vanishing.
        """
        parts = [
            FieldSplitPart(
                fields=["impressions"],
                records=[
                    {"ad_id": "a1", "date_start": "2024-01-01", "impressions": 1},
                    {"ad_id": "a2", "date_start": "2024-01-01", "impressions": 2},
                ],
            ),
            FieldSplitPart(
                fields=["clicks"],
                records=[{"ad_id": "a1", "date_start": "2024-01-01", "clicks": 10}],
            ),
        ]
        merged = job_manager._merge_by_primary_key(parts, ["ad_id", "date_start"])

        assert len(merged) == 2
        by_ad = {record["ad_id"]: record for record in merged}
        assert by_ad["a2"]["impressions"] == 2
        assert "clicks" not in by_ad["a2"]

    def test_breakdown_rows_of_one_entity_day_stay_distinct(
        self, job_manager: FacebookInsightsJobManager
    ):
        """The case field splitting exists for: one ad, one day, many segments.

        A single ad-day yields one row per age x gender combination, so joining
        on only the requestable keys would collapse them all into one record
        carrying an arbitrary segment's metrics.
        """
        segments = [("25-34", "male"), ("25-34", "female"), ("35-44", "male")]

        def part(metric: str, multiplier: int) -> list[dict]:
            return [
                {
                    "account_id": "acct1",
                    "ad_id": "ad1",
                    "date_start": "2024-01-01",
                    "age": age,
                    "gender": gender,
                    metric: (index + 1) * multiplier,
                }
                for index, (age, gender) in enumerate(segments)
            ]

        merged = job_manager._merge_by_primary_key(
            [
                FieldSplitPart(fields=["impressions"], records=part("impressions", 1)),
                FieldSplitPart(fields=["clicks"], records=part("clicks", 10)),
            ],
            job_manager._join_key(AdsInsightsAgeAndGender),
        )

        assert len(merged) == len(segments)
        by_segment = {(r["age"], r["gender"]): r for r in merged}
        for index, segment in enumerate(segments):
            record = by_segment[segment]
            assert record["impressions"] == index + 1
            assert record["clicks"] == (index + 1) * 10

    def test_no_parts_yields_no_records(
        self, job_manager: FacebookInsightsJobManager
    ):
        """Every field unfetchable means there is nothing to emit."""
        assert job_manager._merge_by_primary_key([], ["ad_id"]) == []


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
