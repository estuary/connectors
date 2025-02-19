from datetime import datetime, UTC, timedelta

import pycron

CONNECTOR_RESTART_INTERVAL = timedelta(hours=24)
SMALLEST_CRON_INCREMENT = timedelta(minutes=1)


# next_fire returns the earliest datetime between start and end (exclusive) that matches the cron expression.
def next_fire(
    cron_expression: str,
    start: datetime,
    end: datetime = datetime.now(tz=UTC) + CONNECTOR_RESTART_INTERVAL
) -> datetime | None:
    if not cron_expression:
        return None

    dt = start.replace(second=0, microsecond=0)

    while dt < end:
        dt += SMALLEST_CRON_INCREMENT
        if pycron.is_now(cron_expression, dt):
            return dt

    return None
