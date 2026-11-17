from datetime import datetime, UTC, timedelta

import pycron  # pyright: ignore[reportMissingTypeStubs]  # no stubs exist for this package

CONNECTOR_RESTART_INTERVAL = timedelta(hours=24)
SMALLEST_CRON_INCREMENT = timedelta(minutes=1)


# next_fire returns the earliest datetime after start and at or before end that
# matches the cron expression. Including end means next_fire(expr, last, now)
# and next_fire(expr, now) together cover every fire time.
def next_fire(
    cron_expression: str,
    start: datetime,
    end: datetime | None = None,
) -> datetime | None:
    if not cron_expression:
        return None

    # Computed per call: as a parameter default this was evaluated once, when
    # the module was imported, so the window never advanced in a long-running
    # connector.
    if end is None:
        end = datetime.now(tz=UTC) + CONNECTOR_RESTART_INTERVAL

    dt = start.replace(second=0, microsecond=0) + SMALLEST_CRON_INCREMENT

    while dt <= end:
        if pycron.is_now(cron_expression, dt):
            return dt
        dt += SMALLEST_CRON_INCREMENT

    return None
