import re
from datetime import datetime, UTC
from urllib.parse import urlparse, parse_qs


def extract_next_cursor(headers) -> str | None:
    """Extract the cursor query parameter from a Link header with rel="next".

    Greenhouse v3 API returns pagination links as:
        <https://harvest.greenhouse.io/v3/interviewers?cursor=abc>; rel="next"

    Returns just the cursor value (e.g. "abc"), or None if there is no next page.
    """
    link_header = headers.get("Link", "")
    if not link_header:
        return None

    match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
    if not match:
        return None

    url = match.group(1)
    cursor = parse_qs(urlparse(url).query).get("cursor")
    return cursor[0] if cursor else None


def updated_at_param(
    gt: datetime | None = None,
    gte: datetime | None = None,
    lt: datetime | None = None,
    lte: datetime | None = None,
) -> str:
    """Build a pipe-delimited updated_at query parameter value.

    At least one bound must be provided.

    Example: updated_at_param(gte=start, lte=end) -> "gte|2025-03-01T00:00:00Z|lte|2025-03-20T00:00:00Z"
    """
    if gt is not None and gte is not None:
        raise ValueError("Cannot specify both gt and gte")
    if lt is not None and lte is not None:
        raise ValueError("Cannot specify both lt and lte")

    parts: list[str] = []
    if gt is not None:
        parts.append(f"gt|{dt_to_str(gt)}")
    if gte is not None:
        parts.append(f"gte|{dt_to_str(gte)}")
    if lt is not None:
        parts.append(f"lt|{dt_to_str(lt)}")
    if lte is not None:
        parts.append(f"lte|{dt_to_str(lte)}")

    if not parts:
        raise ValueError("updated_at_param requires at least one bound (gt, gte, lt, or lte)")

    return "|".join(parts)


def dt_to_str(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def now() -> datetime:
    """
    Returns the current time.
    """
    return datetime.now(tz=UTC)
