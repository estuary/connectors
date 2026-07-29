"""
CleverTap fetch + transform, ported verbatim from push_clevertap.py.

The only change is the sink: instead of POSTing to an HTTP Ingest capture, the
records are `yield`ed to the Flow runtime. `events_data` is the driver binding;
each raw record produces one EventsData doc (yielded directly) and one ProfileData
doc (yielded as an AssociatedDocument targeting the profile binding). So the whole
pipeline is a single CleverTap scan — no extra API calls for profiles.
"""

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.capture.document import AssociatedDocument
from estuary_cdk.http import HTTPError, HTTPSession

from .models import EndpointConfig, EventsData, ProfileData

BATCH_SIZE = 50000


def _headers(config: EndpointConfig) -> dict[str, str]:
    return {
        "X-CleverTap-Account-Id": config.account_id,
        "X-CleverTap-Passcode": config.secret,
        "Content-Type": "application/json",
    }


def _to_yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


# --------------------------------------------------------------------------- #
# Transforms (identical mapping to the Fivetran connector / push_clevertap.py)
# --------------------------------------------------------------------------- #
def _convert_to_ist(timestamp_str, log: Logger) -> str | None:
    try:
        return datetime.strptime(str(timestamp_str), "%Y%m%d%H%M%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except Exception as e:
        log.warning(f"Failed to convert timestamp {timestamp_str}: {e}")
        return None


def _build_profile(record: dict) -> ProfileData:
    profile = record.get("profile", {})
    pd_ = profile.get("profileData", {})
    return ProfileData(
        clevertapId=str(profile.get("objectId", "")),
        name=str(profile.get("name", "")),
        identity=str(profile.get("identity", "")),
        platform=str(profile.get("platform", "")),
        phone=str(profile.get("phone", "")),
        role=str(pd_.get("role", "")),
        os=str(pd_.get("os", "")),
        device=str(pd_.get("device", "")),
        make=str(pd_.get("make", "")),
        brand=str(pd_.get("brand", "")),
        sdklevel=str(pd_.get("sdklevel")),
        accountid=str(pd_.get("accountid")),
        appversioncode=str(pd_.get("appversioncode")),
        language=str(pd_.get("language", "")),
        appversion=str(pd_.get("appversion", "")),
        cspid=str(pd_.get("cspid", "")),
        model=str(pd_.get("model", "")),
        osversion=str(pd_.get("osversion", "")),
    )


def _build_event(record: dict, event_name: str, log: Logger) -> EventsData:
    clevertap_id = str(record.get("profile", {}).get("objectId", ""))
    raw_ts = str(record.get("ts", ""))
    properties = json.dumps(record.get("event_props", {}), sort_keys=True)
    timestamp_ist = _convert_to_ist(record.get("ts", ""), log)

    # Deterministic id so overlapping (re-pulled) days upsert instead of duplicate.
    event_id = hashlib.sha256(
        "|".join([clevertap_id, raw_ts, str(event_name), properties]).encode("utf-8")
    ).hexdigest()

    return EventsData(
        event_id=event_id,
        clevertapId=clevertap_id,
        timestamp=timestamp_ist,
        event_name=str(event_name),
        properties=properties,
    )


# --------------------------------------------------------------------------- #
# CleverTap two-phase fetch (POST -> cursor, then GET-paginate next_cursor)
# --------------------------------------------------------------------------- #
async def _iter_pages(
    http: HTTPSession,
    config: EndpointConfig,
    event_name: str,
    from_int: int,
    to_int: int,
    log: Logger,
) -> AsyncGenerator[list[dict], None]:
    """Yields one CleverTap page (list of records) at a time."""
    headers = _headers(config)
    params = {"batch_size": str(BATCH_SIZE), "app": "false", "events": "false", "profile": "true"}
    body = {"from": from_int, "to": to_int, "event_name": event_name}

    log.info(
        f"[{event_name}] POST mint-cursor {config.api_base_url} "
        f"params={params} body={body}"
    )
    resp = json.loads(
        await http.request(
            log, config.api_base_url, method="POST",
            params=params, json=body, headers=headers, with_token=False,
        )
    )
    cursor = resp.get("cursor")
    if not cursor:
        # 200 OK but no cursor => CleverTap accepted the request but returned no
        # data handle (e.g. status:partial/fail). Surface the whole body.
        log.warning(f"[{event_name}] no cursor returned; response={resp}")
        return
    log.info(f"[{event_name}] cursor minted (…{str(cursor)[-8:]}); paginating")

    page_num = 0
    while cursor:
        page = json.loads(
            await http.request(
                log, config.api_base_url, method="GET",
                params={"cursor": cursor}, headers=headers, with_token=False,
            )
        )
        records = page.get("records", [])
        page_num += 1
        log.info(f"[{event_name}] page {page_num}: {len(records)} records")
        if not records:
            break
        yield records
        cursor = page.get("next_cursor")
    log.info(f"[{event_name}] pagination complete after {page_num} page(s)")


# --------------------------------------------------------------------------- #
# Incremental driver (bound to the events_data binding)
# --------------------------------------------------------------------------- #
async def fetch_changes(
    http: HTTPSession,
    config: EndpointConfig,
    profile_binding_index: int | None,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[EventsData | AssociatedDocument | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    tz = ZoneInfo(config.timezone)
    now = datetime.now(tz=UTC)
    today = now.astimezone(tz).date()
    from_date = log_cursor.astimezone(tz).date() - timedelta(days=config.advanced.lookback_days)

    # CleverTap rejects a future `to`; cap at today. from/to are inclusive by day.
    # If end_date is configured, stop there (bounded historical backfill).
    to_date = today
    if config.advanced.end_date:
        end = datetime.strptime(config.advanced.end_date, "%Y-%m-%d").date()
        to_date = min(to_date, end)
    if from_date > to_date:
        from_date = to_date
    from_int, to_int = _to_yyyymmdd(from_date), _to_yyyymmdd(to_date)

    log.info(
        "fetch_changes START: "
        f"cursor={log_cursor.isoformat()} tz={config.timezone} "
        f"range={from_int}..{to_int} (inclusive) "
        f"events={len(config.event_names)} profiles_routed={profile_binding_index is not None}"
    )

    # Checkpoint after every page so progress is visible/durable during a long
    # scan. Cursors must be strictly increasing, so bump a local monotonic clock.
    last = log_cursor

    def _next_cursor() -> datetime:
        nonlocal last
        last = max(datetime.now(tz=UTC), last + timedelta(milliseconds=1))
        return last

    total_events = 0
    total_profiles = 0
    skipped: list[str] = []
    for event_name in config.event_names:
        event_count = 0
        try:
            async for records in _iter_pages(http, config, event_name, from_int, to_int, log):
                for record in records:
                    if profile_binding_index is not None:
                        yield AssociatedDocument(
                            doc=_build_profile(record), binding=profile_binding_index
                        )
                        total_profiles += 1
                    yield _build_event(record, event_name, log)
                    event_count += 1
                    total_events += 1
                yield _next_cursor()  # checkpoint this page's docs
                log.info(f"[{event_name}] checkpoint — {event_count} events so far")
        except HTTPError as e:
            # CleverTap returns 400 {"error":"[\"Invalid event : <name>\"]"} for an
            # unknown event. Optionally skip it and carry on with the rest.
            if config.advanced.skip_invalid_events and e.code == 400 and "Invalid event" in str(e):
                log.warning(f"[{event_name}] SKIPPING — CleverTap reports invalid event")
                skipped.append(event_name)
                continue
            raise
        log.info(f"[{event_name}] DONE: {event_count} events")

    if skipped:
        log.warning(f"Skipped {len(skipped)} invalid event(s): {skipped}")

    log.info(
        f"fetch_changes END: emitted {total_events} events + {total_profiles} profiles; "
        f"advancing cursor. Next run in ~24h (interval=P1D)."
    )
    # Always emit a final checkpoint (covers the zero-records case too).
    yield _next_cursor()


async def noop_fetch(
    log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ProfileData | LogCursor, None]:
    """profile_data has no independent source; it only receives AssociatedDocuments
    emitted by the events_data driver, so its own fetch yields nothing."""
    return
    yield  # pragma: no cover - makes this an async generator
