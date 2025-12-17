"""Factory functions for creating mock Facebook API responses."""

import json
from typing import Any

from estuary_cdk.http import HTTPError


def create_job_submission_response(job_id: str) -> dict[str, str]:
    """Create mock job submission response.

    Args:
        job_id: The job ID to return

    Returns:
        {"report_run_id": job_id}
    """
    return {"report_run_id": job_id}


def create_job_status_response(
    job_id: str,
    status: str = "Job Completed",
    percent: int = 100,
) -> dict[str, Any]:
    """Create mock job status response.

    Args:
        job_id: The job ID
        status: Job status string (e.g., "Job Completed", "Job Running", "Job Failed")
        percent: Completion percentage (0-100)

    Returns:
        {"id": job_id, "async_status": status, "async_percent_completion": percent}
    """
    return {
        "id": job_id,
        "async_status": status,
        "async_percent_completion": percent,
    }


def create_insights_response(
    data: list[dict[str, Any]],
    next_page: str | None = None,
) -> dict[str, Any]:
    """Create mock paginated insights response.

    Args:
        data: List of insight records
        next_page: Optional URL for next page of results

    Returns:
        {"data": [...], "paging": {"next": ...}} if next_page provided
        {"data": [...]} otherwise
    """
    response: dict[str, Any] = {"data": data}
    if next_page:
        response["paging"] = {"next": next_page}
    return response


def create_discovery_response(entity_ids: list[str], id_field: str) -> dict[str, Any]:
    """Create response for entity ID discovery queries.

    Args:
        entity_ids: List of entity IDs to return
        id_field: Field name for IDs (e.g., "campaign_id", "adset_id", "ad_id")

    Returns:
        {"data": [{id_field: id1}, {id_field: id2}, ...]}
    """
    return {"data": [{id_field: eid} for eid in entity_ids]}


def create_data_limit_http_error() -> HTTPError:
    """Create HTTPError for data limit exceeded (code=100, subcode=1487534).

    This error triggers immediate job splitting without retries.

    Returns:
        HTTPError with Facebook's data limit error structure
    """
    return HTTPError(
        message=(
            "Encountered HTTP error status 400 which cannot be retried.\n"
            "URL: https://graph.facebook.com/v21.0/act_123/insights\n"
            "Response:\n"
            '{"error": {"message": "Please reduce the amount of data you\'re asking for", '
            '"type": "OAuthException", "code": 100, "error_subcode": 1487534}}'
        ),
        code=400,
    )


def create_facebook_api_http_error(
    code: int = 190,
    error_subcode: int | None = None,
    message: str = "Invalid OAuth access token",
) -> HTTPError:
    """Create HTTPError for generic Facebook API errors.

    Args:
        code: Facebook error code
        error_subcode: Optional error subcode
        message: Error message

    Returns:
        HTTPError with Facebook's error structure
    """
    error_dict: dict[str, Any] = {
        "message": message,
        "type": "OAuthException",
        "code": code,
    }
    if error_subcode:
        error_dict["error_subcode"] = error_subcode
    return HTTPError(
        message=(
            f"Encountered HTTP error status 400 which cannot be retried.\n"
            f"URL: https://graph.facebook.com/v21.0/act_123/insights\n"
            f"Response:\n"
            f'{json.dumps({"error": error_dict})}'
        ),
        code=400,
    )


def verify_filtering_params(
    request: dict[str, Any],
    expected_field: str,
    expected_ids: list[str],
) -> None:
    """Verify a job request includes correct filtering params.

    Args:
        request: Request dict from MockHTTPSession
        expected_field: Expected filter field (e.g., "campaign.id")
        expected_ids: Expected entity IDs in filter

    Raises:
        AssertionError: If filtering params don't match expected values
    """
    assert "params" in request
    assert request["params"] is not None
    assert "filtering" in request["params"]
    filtering = json.loads(request["params"]["filtering"])
    assert len(filtering) == 1
    assert filtering[0]["field"] == expected_field
    assert filtering[0]["operator"] == "IN"
    assert set(filtering[0]["value"]) == set(expected_ids)
