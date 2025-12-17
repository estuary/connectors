"""Error types and parsing for Facebook Insights jobs."""

import json
from typing import TypeAlias

from pydantic import ValidationError
from estuary_cdk.http import HTTPError

from ..client import FacebookAPIError, FacebookError


# Facebook error codes indicating query scope is too broad.
# These are surfaced as 400 errors by the API.
DATA_LIMIT_ERROR_CODE: int = 100
DATA_LIMIT_ERROR_SUBCODE: int = 1487534


class CannotSplitFurtherError(Exception):
    """
    Raised when a job at the atomic level (single ad) cannot be split.

    This indicates an unrecoverable failure for the current sync.
    The cursor will remain unchanged, allowing retry on the next run.
    """

    pass


class DataLimitExceededError(Exception):
    """
    Raised when Facebook returns error_code=100, error_subcode=1487534.

    This error indicates the query scope is inherently too broad and
    retrying will not help. The job should be split immediately without
    exhausting the retry budget.
    """

    pass


# Type alias for clearer function signatures
ParsedFacebookError: TypeAlias = DataLimitExceededError | FacebookAPIError


def try_parse_facebook_api_error(error: HTTPError) -> ParsedFacebookError | None:
    """
    Try to parse an HTTPError into a typed Facebook exception.

    Returns a DataLimitExceededError for data limit errors (code 100, subcode 1487534),
    a FacebookAPIError for other recognized Facebook errors, or None if the response
    cannot be parsed as a Facebook error.
    """
    try:
        if "Response:" not in error.message:
            return None
        response_json = error.message.split("Response:", 1)[1].strip()
        data = json.loads(response_json)
        if "error" not in data:
            return None
        fb_error = FacebookError.model_validate(data["error"])

        if (
            fb_error.code == DATA_LIMIT_ERROR_CODE
            and fb_error.error_subcode == DATA_LIMIT_ERROR_SUBCODE
        ):
            return DataLimitExceededError(
                f"Facebook API Error ({fb_error.code}): {fb_error.message}"
            )

        return FacebookAPIError(fb_error, error.code)
    except (IndexError, json.JSONDecodeError, ValidationError):
        return None
