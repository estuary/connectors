from datetime import datetime, timedelta, UTC
from time import sleep
from decimal import Decimal
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator
import polars as pl
import json
import requests
import re


from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession
from .models import RestResponse, BulkJobResponse

BASE_URL = "https://abuble-dev-ed.develop.my.salesforce.com" # TODO get this from config
API_VERSION = "v57.0"

async def fetch_incremental_rest(
    cls,
    stream_name,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

    url = f"{BASE_URL}/services/data/{API_VERSION}/query"
    # Salesforce API limits queries that call all fields,
    # But not queries that individually calls all fields.
    # So, we are getting all field names.
    describe_url = f"{BASE_URL}/services/data/v57.0/sobjects/{stream_name}/describe"
    describe_names = []
    max_ts = log_cursor
    log_date = datetime.strftime(log_cursor, "%Y-%m-%dT%H:%M:%S.%fZ")

    resp = await http.request(log, describe_url, method="GET")

    for field in json.loads(resp)["fields"]:
        describe_names.append(field["name"])


    # Even tought Salesforce allows for a WHERE clause, we still filter 
    # Each record by their updated_on field as a redundancy
    query = f'q=SELECT+{', '.join(describe_names)}+from+{stream_name}+WHERE+SystemModstamp+>+{log_date}+ORDER BY+SystemModstamp'

    iterating = True

    while iterating:

        result = RestResponse.model_validate_json(
            await http.request(log, url, method="GET", params=query)
        )
        log.info(f"{result}")

        if result.totalSize == 0:
            # Checking if the query actually returns data
            iterating = False

        for results in result.records:
            updated_on = datetime.strptime(results["SystemModstamp"], "%Y-%m-%dT%H:%M:%S.%f%z")

            if updated_on > log_cursor:
                doc = cls.model_validate(results)
                max_ts = updated_on
                yield doc
            else:
                iterating = False
                break

        if result.done is False:
            # Pagination in case iterating isnt False yet
            url = BASE_URL + result.nextRecordsUrl
            query = None 
        elif result.done is True:
            iterating = False



    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)

async def fetch_backfill_rest(
    cls,
    stream_name,
    stop_date,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:


    if page:
        url = BASE_URL + page
        query = None
    else:
        # Salesforce API limits queries that call all fields,
        # But not queries that individually calls all fields.
        # So, we are getting all field names.
        url = f"{BASE_URL}/services/data/{API_VERSION}/query"
        describe_url = f"{BASE_URL}/services/data/v57.0/sobjects/{stream_name}/describe"
        describe_names = []
        cutoff_date = datetime.strftime(cutoff, "%Y-%m-%dT%H:%M:%S.%fZ")


        resp = await http.request(log, describe_url, method="GET")

        for field in json.loads(resp)["fields"]:
            describe_names.append(field["name"])


        # Even tought Salesforce allows for a WHERE clause, we still filter 
        # Each record by their updated_on field as a redundancy
        query = f'q=SELECT+{', '.join(describe_names)}+from+{stream_name}+WHERE+CreatedDate+<+{cutoff_date}+ORDER BY+CreatedDate'

    result = RestResponse.model_validate_json(
            await http.request(log, url, method="GET", params=query)
        )
    if result.totalSize == 0:
        # Checking if the query actually returns data
        return

    for results in result.records:
        created_on = datetime.strptime(results["CreatedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")

        
        if created_on == stop_date:
            doc = cls.model_validate(results)
            yield doc
            return
        
        elif created_on < stop_date:
            return
        
        elif created_on < cutoff:
            doc = cls.model_validate(results)
            yield doc

    if result.done is False:
        yield result.nextRecordsUrl

    else:
        return

async def fetch_incremental_bulk(
    cls,
    stream_name,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

    url = f"{BASE_URL}/services/data/{API_VERSION}/jobs/query"
    describe_url = f"{BASE_URL}/services/data/v57.0/sobjects/{stream_name}/describe"
    describe_names = []
    max_ts = log_cursor
    log_date = datetime.strftime(log_cursor, "%Y-%m-%dT%H:%M:%S.%fZ")
    resp = await http.request(log, describe_url, method="GET")

    for field in json.loads(resp)["fields"]:
        if field["aggregatable"] is False:
            continue
        describe_names.append(field["name"])

    # BULK API does not allow for ORDER BY statements
    # Because of that, we need to search on all 
    # BULK results to be sure we are not missing data.

    query = f'SELECT {', '.join(describe_names)} from {stream_name} WHERE SystemModstamp > {log_date}'
    payload = {
        "operation": "query",
        "query": query,
        "contentType": "CSV",
        "columnDelimiter": "COMMA",
        "lineEnding": "LF"
        }

    result = BulkJobResponse.model_validate_json(
            await http.request(log, url, method="POST", json=payload)
        )
    
    job_id = result.id
    job_url = url + f"/{job_id}"
    # Salesforce job can take up to 24 hours to be ready
    job_max_time = datetime.now(tz=UTC) + timedelta(hours=24) # TODO: allow the user to set this (?)
    
    while datetime.now(tz=UTC) < job_max_time:
        job_status = BulkJobResponse.model_validate_json(
            await http.request(log, job_url, method="GET")
        )
        log.info(f"{job_status.state}")

        if job_status.state in ["Aborted", "Failed"]:
                raise
                #TODO handle errors here
        elif job_status.state in ["InProgress", "UploadComplete"]:
            sleep(10)
            continue
        
        elif job_status.state == "JobComplete":
            break
    else:
        raise # TODO add raise to explain that the time limit has passed

    iterating = True
    results_url = job_url + "/results"
    result_params = None

    while iterating:
        response_headers = requests.get(results_url,params=result_params,headers={"Authorization": f"Bearer {http.token_source._access_token.access_token}"}).headers
        data_result = await http.request(log, results_url, method="GET", params=result_params)
        # data_result is a CSV response ( the only one allowed on BULK API as of now )
        # so im using polars to help handle the data correctly
        df = pl.read_csv(data_result)
        df = df.with_columns(pl.col(pl.Utf8).replace("", None)) # Filter empty strings

        for data in df.to_dicts():
            updated_on = datetime.strptime(data["SystemModstamp"], "%Y-%m-%dT%H:%M:%S.%f%z")
            if updated_on > log_cursor:
                doc = cls.model_validate_json(json.dumps(data))
                max_ts = updated_on
                yield doc
            else:
                continue

        if response_headers["Sforce-Locator"] == "null": # If this header is null, there are no more BULK results
            iterating = False

        else:
            result_params = f"locator={response_headers["Sforce-Locator"]}"


    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)

async def fetch_backfill_bulk(
    cls,
    stream_name,
    stop_date,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:

    if page:
        response = requests.get(page,headers={"Authorization": f"Bearer {http.token_source._access_token.access_token}"})
        data_result = response.content
        response_headers = response.headers
        df = pl.read_csv(data_result)
        df = df.with_columns(pl.col(pl.Utf8).replace("", None))

        for data in df.to_dicts():
            created_on = datetime.strptime(data["CreatedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")

            if created_on == stop_date:
                doc = cls.model_validate_json(json.dumps(data))
                yield doc
            
            elif created_on < stop_date:
                continue
            
            elif created_on < cutoff:
                doc = cls.model_validate_json(json.dumps(data))
                yield doc

        if response_headers["Sforce-Locator"] == "null":
            return

        else:
            results_url = re.sub(r"&locator=(.*)",f"&locator={response_headers["Sforce-Locator"]}",response.url)
            yield results_url

    else:
        url = f"{BASE_URL}/services/data/{API_VERSION}/jobs/query"
        describe_url = f"{BASE_URL}/services/data/v57.0/sobjects/{stream_name}/describe"
        describe_names = []
        cutoff_date = datetime.strftime(cutoff, "%Y-%m-%dT%H:%M:%S.%fZ")
        resp = await http.request(log, describe_url, method="GET")

        for field in json.loads(resp)["fields"]:
            if field["aggregatable"] is False:
                continue
            describe_names.append(field["name"])
        query = f'SELECT {', '.join(describe_names)} from {stream_name} WHERE CreatedDate < {cutoff_date}'
        payload = {
            "operation": "query",
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF"
            }

        result = BulkJobResponse.model_validate_json(
                await http.request(log, url, method="POST", json=payload)
            )
        
        job_id = result.id
        job_url = url + f"/{job_id}"
        job_max_time = datetime.now(tz=UTC) + timedelta(hours=24) # TODO: allow the user to set this (?)

        while datetime.now(tz=UTC) < job_max_time:
            job_status = BulkJobResponse.model_validate_json(
                await http.request(log, job_url, method="GET")
            )
            log.info(f"{job_status.state}")

            if job_status.state in ["Aborted", "Failed"]:
                    raise
                    #TODO handle errors here
            elif job_status.state in ["InProgress", "UploadComplete"]:
                sleep(10)
                continue
            
            elif job_status.state == "JobComplete":
                break
        else:
            raise # TODO add raise to explain that the time limit has passed


        # For testing pourposes, ive set a limit of 10 records per BULK result
        results_url = job_url + "/results?maxRecords=10"

        response = requests.get(results_url,headers={"Authorization": f"Bearer {http.token_source._access_token.access_token}"})
        response_headers = response.headers

        data_result = response.content
        df = pl.read_csv(data_result)
        df = df.with_columns(pl.col(pl.Utf8).replace("", None))

        for data in df.to_dicts():
            created_on = datetime.strptime(data["CreatedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")

            if created_on == stop_date:
                doc = cls.model_validate_json(json.dumps(data))
                yield doc
            
            elif created_on < stop_date:
                continue
            
            elif created_on < cutoff:
                doc = cls.model_validate_json(json.dumps(data))
                yield doc

        if response_headers["Sforce-Locator"] == "null":
            return

        else:
            yield results_url + f"&locator={response_headers["Sforce-Locator"]}"

