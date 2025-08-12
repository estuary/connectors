from datetime import UTC, datetime, timedelta

import pytest

from source_klaviyo_native.shared import split_date_window


def assert_chunks_valid(chunks: list[tuple[datetime, datetime]], start: datetime, end: datetime):
    """Helper to validate basic chunk properties: length, boundaries, and contiguity."""
    assert chunks[0][0] == start
    assert chunks[-1][1] == end

    for i in range(len(chunks) - 1):
        assert chunks[i][1] == chunks[i + 1][0]


def assert_microsecond_boundaries(chunks: list[tuple[datetime, datetime]]):
    """Helper to verify intermediate chunk boundaries have no microseconds."""
    for i in range(len(chunks) - 1):
        chunk_end = chunks[i][1]
        assert chunk_end.microsecond == 0, f"Chunk boundary {chunk_end} should have no microseconds"


def assert_total_duration_preserved(chunks: list[tuple[datetime, datetime]], expected_duration: timedelta):
    """Helper to verify total duration is preserved across all chunks."""
    total_duration = sum(
        (chunk_end - chunk_start for chunk_start, chunk_end in chunks),
        timedelta()
    )
    assert total_duration == expected_duration


def assert_chunks_roughly_equal(chunks: list[tuple[datetime, datetime]], expected_duration: timedelta, tolerance:timedelta = timedelta(seconds=1)):
    """Helper to verify all chunks are roughly the expected duration."""
    for chunk_start, chunk_end in chunks:
        duration = chunk_end - chunk_start
        assert abs(duration - expected_duration) < tolerance


def assert_chunks_minimum_duration(chunks: list[tuple[datetime, datetime]], min_duration: timedelta):
    """Helper to verify all chunks are at least the minimum duration."""
    for chunk_start, chunk_end in chunks:
        duration = chunk_end - chunk_start
        assert duration >= min_duration


def test_split_date_window_basic():
    """Test basic functionality with 10 chunks."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 11, 0, 0, 0, tzinfo=UTC)

    chunks = split_date_window(start, end, 10)

    assert len(chunks) == 10
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)
    assert_chunks_roughly_equal(chunks, timedelta(days=1))


def test_split_date_window_single_chunk():
    """Test with a single chunk."""
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC)
    
    chunks = split_date_window(start, end, 1)
    
    assert len(chunks) == 1
    assert chunks[0] == (start, end)


def test_split_date_window_small_duration():
    """Test with a very small time window."""
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 12, 0, 10, tzinfo=UTC)

    chunks = split_date_window(start, end, 5)

    assert len(chunks) == 5
    assert_chunks_valid(chunks, start, end)


def test_split_date_window_with_microseconds():
    """Test microsecond precision handling and that chunk boundaries are rounded to seconds."""
    start = datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    end = datetime(2024, 1, 1, 12, 0, 1, 234567, tzinfo=UTC)

    chunks = split_date_window(start, end, 3)

    assert len(chunks) == 3
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)


def test_split_date_window_error_cases():
    """Test error conditions."""
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)

    # Test zero chunks
    with pytest.raises(ValueError, match="num_chunks must be positive"):
        split_date_window(start, end, 0)

    # Test negative chunks
    with pytest.raises(ValueError, match="num_chunks must be positive"):
        split_date_window(start, end, -1)

    # Test start >= end
    with pytest.raises(ValueError, match="start must be before end"):
        split_date_window(end, start, 5)

    # Test start == end
    with pytest.raises(ValueError, match="start must be before end"):
        split_date_window(start, start, 5)


def test_split_date_window_large_number_of_chunks():
    """Test with a large number of chunks."""
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)

    chunks = split_date_window(start, end, 1000)

    assert len(chunks) == 1000
    assert_chunks_valid(chunks, start, end)
    assert_total_duration_preserved(chunks, end - start)


def test_split_date_window_uneven_division():
    """Test when the duration doesn't divide evenly."""
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 0, 1, tzinfo=UTC)

    chunks = split_date_window(start, end, 10)

    assert len(chunks) == 10
    assert_chunks_valid(chunks, start, end)
    assert_total_duration_preserved(chunks, end - start)


def test_split_date_window_minimum_chunk_size_no_constraint():
    """Test minimum_chunk_size parameter when it doesn't constrain the result."""
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 11, 0, 0, 0, tzinfo=UTC)

    # Request 5 chunks with minimum size of 1 day - should get 5 chunks
    chunks = split_date_window(start, end, 5, minimum_chunk_size=timedelta(days=1))

    assert len(chunks) == 5
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)
    assert_chunks_roughly_equal(chunks, timedelta(days=2))


def test_split_date_window_minimum_chunk_size_reduces_chunks():
    """Test minimum_chunk_size parameter when it reduces the number of chunks."""
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 11, 0, 0, 0, tzinfo=UTC)

    # Request 10 chunks but minimum size is 3 days - should get 3 chunks
    chunks = split_date_window(start, end, 10, minimum_chunk_size=timedelta(days=3))

    assert len(chunks) == 3  # 10 days / 3 days = 3.33, so 3 chunks
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)
    assert_chunks_minimum_duration(chunks, timedelta(days=3))


def test_split_date_window_minimum_chunk_size_larger_than_total():
    """Test minimum_chunk_size when it's larger than the total duration."""
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 3, 0, 0, 0, tzinfo=UTC)

    # Request 5 chunks but minimum size is 10 days - should get 1 chunk
    chunks = split_date_window(start, end, 5, minimum_chunk_size=timedelta(days=10))

    assert len(chunks) == 1
    assert chunks[0] == (start, end)
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)


def test_split_date_window_minimum_chunk_size_ensures_contiguity():
    """Test that minimum_chunk_size maintains contiguity across the entire window."""
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 2, 15, 30, 0, tzinfo=UTC)  # 27.5 hours

    # Request 10 chunks with minimum size of 6 hours - should get 4 chunks
    chunks = split_date_window(start, end, 10, minimum_chunk_size=timedelta(hours=6))

    assert len(chunks) == 4  # 27.5 hours / 6 hours = 4.58, so 4 chunks
    assert_chunks_valid(chunks, start, end)
    assert_microsecond_boundaries(chunks)
    assert_total_duration_preserved(chunks, end - start)
    assert_chunks_minimum_duration(chunks, timedelta(hours=6))
