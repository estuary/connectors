from datetime import datetime, timedelta


DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def split_date_window(
    start: datetime,
    end: datetime,
    num_chunks: int,
    minimum_chunk_size: timedelta | None = None
) -> list[tuple[datetime, datetime]]:
    """Split a date window into equal-sized chunks.

    Args:
        start: Start datetime of the window
        end: End datetime of the window  
        num_chunks: Number of chunks to split into
        minimum_chunk_size: Optional minimum size for each chunk. If provided,
            chunks will be at least this large, and may result in fewer than num_chunks.

    Returns:
        List of (start, end) datetime tuples representing each chunk.
        All chunk boundaries are rounded to second-level granularity.
        Chunks remain contiguous across the entire start-end window.
    """
    if num_chunks <= 0:
        raise ValueError("num_chunks must be positive")

    if start >= end:
        raise ValueError("start must be before end")

    total_duration = end - start
    chunk_duration = total_duration / num_chunks

    # If minimum_chunk_size is specified and larger than calculated chunk_duration,
    # adjust the number of chunks to respect the minimum size.
    if minimum_chunk_size is not None and chunk_duration < minimum_chunk_size:
        # Calculate how many chunks we can actually fit with the minimum size.
        actual_num_chunks = int(total_duration / minimum_chunk_size)
        if actual_num_chunks < 1:
            actual_num_chunks = 1
        chunk_duration = total_duration / actual_num_chunks
        num_chunks = actual_num_chunks

    chunks = []
    current_start = start

    for i in range(num_chunks):
        if i == num_chunks - 1:
            # Last chunk goes to the end to handle any rounding.
            chunk_end = end
        else:
            # Calculate the next chunk boundary and round to second-level granularity.
            next_boundary = current_start + chunk_duration
            chunk_end = next_boundary.replace(microsecond=0)

        chunks.append((current_start, chunk_end))
        # Use the rounded chunk_end as the next start to maintain continuity.
        current_start = chunk_end

    return chunks
