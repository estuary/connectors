from datetime import UTC, datetime, timedelta
from logging import Logger

MONDAY_TIMESTAMP_DIVISOR = 10_000_000
TIMESTAMP_VALIDATION_BUFFER_DAYS = 365


def parse_monday_17_digit_timestamp(timestamp_str: str, log: Logger) -> datetime:
    if not timestamp_str:
        raise ValueError("Empty timestamp string for activity log. Cannot parse.")

    try:
        timestamp_17_digit = int(timestamp_str)

        if timestamp_17_digit <= 0:
            raise ValueError(
                f"Invalid timestamp value: {timestamp_17_digit}. Must be positive."
            )

        timestamp_seconds = timestamp_17_digit / MONDAY_TIMESTAMP_DIVISOR
        result = datetime.fromtimestamp(timestamp_seconds, tz=UTC)

        now = datetime.now(UTC)
        if not (
            datetime(1970, 1, 1, tzinfo=UTC)
            <= result
            <= now + timedelta(days=TIMESTAMP_VALIDATION_BUFFER_DAYS)
        ):
            raise ValueError(
                f"Parsed timestamp {result} is out of valid range: "
                f"1970-01-01 to {now + timedelta(days=TIMESTAMP_VALIDATION_BUFFER_DAYS)}"
            )

        return result

    except (ValueError, TypeError, OverflowError) as e:
        log.warning(f"Unable to parse timestamp '{timestamp_str}': {e}")
        raise
    except Exception as e:
        log.error(f"Unexpected error parsing timestamp '{timestamp_str}': {e}")
        raise
