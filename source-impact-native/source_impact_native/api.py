from datetime import datetime, timedelta, UTC
from decimal import Decimal
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator
import json

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)

API = "https://api.impact.com"
API_CATALOG = "Advertisers"

async def fetch_incremental(
    cls,
    account_sid,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:
    """Fetch incremental is the default FetchChangesFn handler for source-impact-native
    This method has the basic behaviour of querying data after the logcursor, comparing and yielding
    records accorrding.
    """
    _cls: Any = cls  # Silence mypy false-positive
    
    iterating = True

    url = f"{API}/{API_CATALOG}/{account_sid}/{_cls.NAME}"
    parameters = None
    headers = {'Accept': 'application/json'} # required by the source, else results are XML

    if _cls.START_DATE_INCREMENTAL:
        parameters = {f"{_cls.START_DATE_INCREMENTAL}": _cursor_dt(_cls.NAME, log_cursor)}
    max_ts = log_cursor

    while iterating:
        result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

        for results in result[f"{_cls.NAME}"]:
            if _s_to_dt(results[f"{_cls.REP_KEY}"]) > log_cursor:
                max_ts = _s_to_dt(results[f"{_cls.REP_KEY}"])
                doc = _cls.model_validate_json(json.dumps(results))
                yield doc
        
            elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < log_cursor:
                iterating = False
                break
        if result.get("@nextpageuri"):
            url = API + result["@nextpageuri"]
        else:
            break
    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1) # startTimestamp is inclusive.


async def fetch_backfill(
    cls,
    stop_date,
    account_sid,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:
    """Fetch backfill is the default FetchPagesFn handler for source-impact-native
    This method has the basic behaviour of backfilling data and (when possible) query data from
    specific ranges. Its page_cursor is already the full URI for the next page
    """

    _cls: Any = cls
    headers = {'Accept': 'application/json'}
    parameters = None

    if page:
        url = API + page
    
    else:
        url = f"{API}/{API_CATALOG}/{account_sid}/{cls.NAME}"
        if _cls.START_DATE:
            parameters = {f"{_cls.START_DATE}": _cursor_dt(cls.NAME, stop_date), f"{_cls.END_DATE}": _cursor_dt(cls.NAME, cutoff)}

    result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

    for results in result[f"{_cls.NAME}"]:
        if _s_to_dt(results[f"{_cls.REP_KEY}"]) == stop_date:
            doc = _cls.model_validate_json(json.dumps(results))
            yield doc
            return
        elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < stop_date:
            return
        elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < cutoff:
            doc = _cls.model_validate_json(json.dumps(results))
            yield doc

    if result.get("@nextpageuri"):
        yield result["@nextpageuri"]
    else:
        return 



async def fetch_incremental_actions(
    cls_parent,
    cls,
    account_sid,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:
    """Fetch incremental actions is the default FetchChangesFn handler for Actions Streams
    This method requires CampaignIds to query for actions data.

    This method does not use the action updates endpoint. Instead, we only call for the basic Actions endpoint, which returns recent data since the last 7 days.
    we do this since the action updates endpoint returns data in a totally diffent object, which does not match the original Actions endpoint.
    """
    headers = {'Accept': 'application/json'}

    campaign_list = set()

    campaigns = fetch_snapshot(cls_parent, account_sid, http, log)
    async for campaign in campaigns:
        campaign_list.add(campaign.Id)

    for campaign in campaign_list:

        iterating = True

        url = f"{API}/{API_CATALOG}/{account_sid}/{cls.NAME}"
        parameters = {"CampaignId": campaign}
        max_ts = log_cursor

        _cls: Any = cls  # Silence mypy false-positive

        while iterating:
            result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))


            for results in result[f"{cls.NAME}"]:
                if _s_to_dt(results[f"CreationDate"]) > log_cursor:
                    max_ts = _s_to_dt(results[f"CreationDate"])
                    doc = _cls.model_validate_json(json.dumps(results))
                    yield doc
            
                elif _s_to_dt(results[f"CreationDate"]) < log_cursor:
                    iterating = False
                    break
            if result.get("@nextpageuri"):
                url = API + result["@nextpageuri"]
            else:
                iterating = False
    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1) # startTimestamp is inclusive.



async def fetch_backfill_actions(
    cls_parent,
    cls,
    stop_date,
    account_sid,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:
    """Fetch backfill actions is the default FetchPageFn handler for Actions Streams
    This method requires CampaignIds to query for actions data.

    For backfilling, the Action streams has a couple of limitations:
    If requesting a start_date, we must provide a end_date
    We cannot request data with a start_date of more than 3 years, and
    start_date and end_date cant be set more than 45 days apart.
    
    """
    headers = {'Accept': 'application/json'}
    parameters = {}


    if cutoff - timedelta(days=1095) <= stop_date <= cutoff:
        start_date = stop_date
        end_date = cutoff
    else:
        start_date = cutoff - timedelta(days=1095)
        end_date = cutoff


    dates = get_date_ranges(start_date, end_date)

    campaign_list = set()

    campaigns = fetch_snapshot(cls_parent, account_sid, http, log)
    async for campaign in campaigns:
        campaign_list.add(campaign.Id)

    for campaign in campaign_list:
        url = f"{API}/{API_CATALOG}/{account_sid}/{cls.NAME}"

        _cls: Any = cls

        parameters["CampaignId"] = campaign

        for start, end in dates:
            iterating = True

            parameters["LockingDateStart"] = _cursor_dt(cls.NAME, start)
            parameters["LockingDateEnd"] = _cursor_dt(cls.NAME, end)


            while iterating:

                result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

                for results in result[f"{_cls.NAME}"]:
                    if _s_to_dt(results[f"CreationDate"]) == stop_date:
                        doc = _cls.model_validate_json(json.dumps(results))
                        yield doc
                        iterating = False
                        break
                    elif _s_to_dt(results[f"CreationDate"]) < stop_date:
                        break
                    elif _s_to_dt(results[f"CreationDate"]) < cutoff:
                        doc = _cls.model_validate_json(json.dumps(results))
                        log.error(f"{doc}")
                        yield doc

                if result.get("@nextpageuri"):
                    url = API + result["@nextpageuri"]
                else:
                    iterating = False
    return

async def fetch_snapshot(
    cls,
    account_sid,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator:
    """Fetch snapshots is the default snapshot handler for source-impact-native"""
    _cls: Any = cls  # Silence mypy false-positive
    
    iterating = True

    url = f"{API}/{API_CATALOG}/{account_sid}/{cls.NAME}"
    parameters = None
    headers = {'Accept': 'application/json'}

    while iterating:
        result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

        for results in result[f"{_cls.NAME}"]:
            doc = _cls.model_validate_json(json.dumps(results))
            yield doc

        if result.get("@nextpageuri"):
            url = API + result["@nextpageuri"]
        else:
            break


async def fetch_incremental_child(
    cls_parent,
    cls,
    account_sid,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:
    """_Fetch incremental child is the default FetchChangesFn handler for source-impact-native child streams.
    This method has the basic behaviour of querying data after the logcursor, comparing and yielding
    records accorrding.
    
    This method requires CampaignId values from the Campaigns stream.
    """

    _cls: Any = cls  # Silence mypy false-positive

    campaign_list = set()

    campaigns = fetch_snapshot(cls_parent, account_sid, http, log)
    async for campaign in campaigns:
        campaign_list.add(campaign.Id)

    

    for campaign in campaign_list:
        iterating = True

        url = f"{API}/{API_CATALOG}/{account_sid}/Campaigns/{campaign}/{cls.NAME}"
        if cls.NAME == "Tasks":
            url = f"{API}/{API_CATALOG}/{account_sid}/Programs/{campaign}/{cls.NAME}"
        parameters = None
        headers = {'Accept': 'application/json'}
        if _cls.START_DATE_INCREMENTAL:
            parameters = {f"{_cls.START_DATE_INCREMENTAL}": _cursor_dt(cls.NAME, log_cursor)}
        max_ts = log_cursor

        while iterating:
            result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

            for results in result[f"{_cls.NAME}"]:
                if _s_to_dt(results[f"{_cls.REP_KEY}"]) > log_cursor:
                    max_ts = _s_to_dt(results[f"{_cls.REP_KEY}"])
                    doc = _cls.model_validate_json(json.dumps(results))
                    yield doc
            
                elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < log_cursor:
                    iterating = False
                    break
            if result.get("@nextpageuri"):
                url = API + result["@nextpageuri"]
            else:
                break
    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1) # startTimestamp is inclusive.


async def fetch_backfill_child(
    cls_parent,
    cls,
    stop_date,
    account_sid,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:
    """_Fetch backfill child is the default FetchPagesFn handler for source-impact-native child streams.
    This method has the basic behaviour of backfilling data and (when possible) query data from
    specific ranges. The API does not allow for a good page_cursor logic ( since we can iterate over multiple campaigns ),
    so, as of now, this method does not yield a valid page_cursor.
    
    This method requires CampaignId values from the Campaigns stream.
    """

    _cls: Any = cls
    headers = {'Accept': 'application/json'}
    parameters = None

    campaign_list = set()

    campaigns = fetch_snapshot(cls_parent, account_sid, http, log)
    async for campaign in campaigns:
        campaign_list.add(campaign.Id)

    for campaign in campaign_list:
        iterating = True

        url = f"{API}/{API_CATALOG}/{account_sid}/Campaigns/{campaign}/{cls.NAME}"
        if cls.NAME == "Tasks":
            url = f"{API}/{API_CATALOG}/{account_sid}/Programs/{campaign}/{cls.NAME}"
        headers = {'Accept': 'application/json'}
        # if _cls.START_DATE:
        #     parameters = {f"{_cls.START_DATE}": _cursor_dt(cls.NAME, stop_date), f"{_cls.END_DATE}": _cursor_dt(cls.NAME, cutoff)}

        while iterating:
            result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

            for results in result[f"{_cls.NAME}"]:
                if _s_to_dt(results[f"{_cls.REP_KEY}"]) == stop_date:
                    doc = _cls.model_validate_json(json.dumps(results))
                    yield doc

                elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < stop_date:
                    iterating = False
                    break
                elif _s_to_dt(results[f"{_cls.REP_KEY}"]) < cutoff:
                    doc = _cls.model_validate_json(json.dumps(results))
                    yield doc
            if result.get("@nextpageuri"):
                url = API + result["@nextpageuri"]
            else:
                iterating = False
    return


async def fetch_snapshot_child(
    cls_parent,
    cls,
    account_sid,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator:
    """Fetch snapshot child works very similar to the base fetch_snapshot method, withe the only difference beeing the campaigns info"""
    _cls: Any = cls  # Silence mypy false-positive

    campaign_list = set()

    campaigns = fetch_snapshot(cls_parent, account_sid, http, log)
    async for campaign in campaigns:
        campaign_list.add(campaign.Id)

    

    for campaign in campaign_list:
    
        iterating = True

        url = f"{API}/{API_CATALOG}/{account_sid}/Campaigns/{campaign}/{cls.NAME}"
        parameters = None
        headers = {'Accept': 'application/json'}

        while iterating:
            result = json.loads(await http.request(log, url, method="GET", params=parameters, headers=headers))

            for results in result["Groups"]:
                doc = _cls.model_validate_json(json.dumps(results))
                yield doc

            if result.get("@nextpageuri"):
                url = API + result["@nextpageuri"]
            else:
                break



def _s_to_dt(date: str):
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    return date

def _cursor_dt(name, logcursor):
    new_logcursor = logcursor.strftime("%Y-%m-%dT%H:%M:%S%z")
    if name == "PhoneNumbers" or name == "Jobs":
        new_logcursor = logcursor.strftime("%Y-%m-%d")
    return new_logcursor

def get_date_ranges(start_date, end_date, step=40):
    current_start = start_date
    date_ranges = []

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=step), end_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end 
    
    return date_ranges