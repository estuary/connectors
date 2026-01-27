import asyncio
import time
from collections import deque
from logging import Logger

from .constants import RateGroup, RATE_LIMITS


class GroupRateLimiter:
    """Rate limiter using a sliding window to track requests per API group.

    Allows concurrent requests up to the rate limit. When the limit is reached,
    new requests wait until the oldest request in the window expires (60 seconds).
    """

    def __init__(self) -> None:
        self._locks = {group: asyncio.Lock() for group in RateGroup}
        self._timestamps: dict[RateGroup, deque[float]] = {
            group: deque() for group in RateGroup
        }

    async def throttle(self, group: RateGroup, log: Logger) -> None:
        """Wait if necessary to stay within rate limits for the given group.

        Tracks request timestamps in a 60-second sliding window. If at the limit,
        sleeps until the oldest request falls outside the window.
        """
        limit = RATE_LIMITS[group]

        while True:
            async with self._locks[group]:
                now = time.monotonic()
                timestamps = self._timestamps[group]

                # Remove timestamps older than 60 seconds
                cutoff = now - 60.0
                while timestamps and timestamps[0] < cutoff:
                    timestamps.popleft()

                # If under limit, record this request and proceed
                if len(timestamps) < limit:
                    timestamps.append(now)
                    return

                # At limit - calculate how long until oldest request expires
                sleep_time = 60.0 - (now - timestamps[0])

            # Sleep outside the lock to allow other requests to queue
            if sleep_time > 0:
                log.debug(
                    f"Rate limit reached for {group} group "
                    f"({len(timestamps)}/{limit}), sleeping {sleep_time:.2f}s"
                )
                await asyncio.sleep(sleep_time)
            # Loop back to re-check (another request may have taken the slot)


_rate_limiter: GroupRateLimiter | None = None


def get_rate_limiter() -> GroupRateLimiter:
    """Return the module-level singleton rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = GroupRateLimiter()
    return _rate_limiter
