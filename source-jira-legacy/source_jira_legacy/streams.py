#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import re
import urllib.parse as urlparse
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib.parse import parse_qsl
from copy import deepcopy

import pendulum
import requests
from airbyte_cdk.logger import AirbyteLogger as Logger
from airbyte_cdk.sources import Source
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from requests.exceptions import HTTPError
from source_jira_legacy.type_transfromer import DateTimeTransformer

from .utils import read_full_refresh, read_incremental, safe_max

API_VERSION = 3


class JiraAvailabilityStrategy(HttpAvailabilityStrategy):
    """
    Inherit from HttpAvailabilityStrategy with slight modification to 403 and 401 error messages.
    """

    def reasons_for_unavailable_status_codes(self, stream: Stream, logger: Logger, source: Source, error: HTTPError) -> Dict[int, str]:
        reasons_for_codes: Dict[int, str] = {
            requests.codes.FORBIDDEN: "Please check the 'READ' permission(Scopes for Connect apps) and/or the user has Jira Software rights and access.",
            requests.codes.UNAUTHORIZED: "Invalid creds were provided, please check your api token, domain and/or email.",
            requests.codes.NOT_FOUND: "Please check the 'READ' permission(Scopes for Connect apps) and/or the user has Jira Software rights and access.",
        }
        return reasons_for_codes


class JiraStream(HttpStream, ABC):
    """
    Jira API Reference: https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/
    """

    page_size = 50
    primary_key: Optional[str] = "id"
    extract_field: Optional[str] = None
    api_v1 = False
    # Defines the HTTP status codes for which the slice should be skipped.
    # Reference issue: https://github.com/airbytehq/oncall/issues/2133
    # we should skip the slice with `board id` which doesn't support `sprints`
    # it's generally applied to all streams that might have the same error hit in the future.
    skip_http_status_codes = [requests.codes.BAD_REQUEST]
    raise_on_http_errors = True
    transformer: TypeTransformer = DateTimeTransformer(TransformConfig.DefaultSchemaNormalization)
    # emitting state message after every page read
    state_checkpoint_interval = page_size

    def __init__(self, domain: str, projects: List[str], **kwargs):
        super().__init__(**kwargs)
        self._domain = domain
        self._projects = projects

    @property
    def url_base(self) -> str:
        if self.api_v1:
            return f"https://{self._domain}/rest/agile/1.0/"
        return f"https://{self._domain}/rest/api/{API_VERSION}/"

    @property
    def availability_strategy(self) -> HttpAvailabilityStrategy:
        return JiraAvailabilityStrategy()

    def _get_custom_error(self, response: requests.Response) -> str:
        """Method for specifying custom error messages for errors that will be skipped."""
        return ""

    @property
    def max_retries(self) -> Union[int, None]:
        """Number of retries increased from default 5 to 10, based on issues with Jira. Max waiting time is still default 10 minutes."""
        return 10

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if isinstance(response_json, dict):
            startAt = response_json.get("startAt")
            if startAt is not None:
                startAt += response_json["maxResults"]
                if "isLast" in response_json:
                    if response_json["isLast"]:
                        return
                elif "total" in response_json:
                    if startAt >= response_json["total"]:
                        return
                return {"startAt": startAt}
        elif isinstance(response_json, list):
            if len(response_json) == self.page_size:
                query_params = dict(parse_qsl(urlparse.urlparse(response.url).query))
                startAt = int(query_params.get("startAt", 0)) + self.page_size
                return {"startAt": startAt}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"maxResults": self.page_size}
        if next_page_token:
            params.update(next_page_token)
        return params

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return {"Accept": "application/json"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json if not self.extract_field else response_json.get(self.extract_field, [])
        if isinstance(records, list):
            for record in records:
                yield self.transform(record=record, **kwargs)
        else:
            yield self.transform(record=records, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        return record

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            yield from super().read_records(**kwargs)
        except HTTPError as e:
            if not (self.skip_http_status_codes and e.response.status_code in self.skip_http_status_codes):
                raise e
            errors = e.response.json().get("errorMessages")
            custom_error = self._get_custom_error(e.response)
            self.logger.warning(f"Stream `{self.name}`. An error occurred, details: {errors}. Skipping for now. {custom_error}")


class StartDateJiraStream(JiraStream, ABC):
    def __init__(
        self,
        start_date: Optional[pendulum.DateTime] = None,
        lookback_window_minutes: pendulum.Duration = pendulum.duration(minutes=0),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._lookback_window_minutes = lookback_window_minutes
        self._start_date = start_date


class IncrementalJiraStream(StartDateJiraStream, ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._starting_point_cache = {}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        updated_state = latest_record[self.cursor_field]
        stream_state_value = current_stream_state.get(self.cursor_field)
        if stream_state_value:
            updated_state = max(updated_state, stream_state_value)
        current_stream_state[self.cursor_field] = updated_state
        return current_stream_state

    def jql_compare_date(self, stream_state: Mapping[str, Any]) -> Optional[str]:
        compare_date = self.get_starting_point(stream_state)
        if compare_date:
            compare_date = compare_date.strftime("%Y/%m/%d %H:%M")
            return f"{self.cursor_field} >= '{compare_date}'"

    def get_starting_point(self, stream_state: Mapping[str, Any]) -> Optional[pendulum.DateTime]:
        if self.cursor_field not in self._starting_point_cache:
            self._starting_point_cache[self.cursor_field] = self._get_starting_point(stream_state=stream_state)
        return self._starting_point_cache[self.cursor_field]

    def _get_starting_point(self, stream_state: Mapping[str, Any]) -> Optional[pendulum.DateTime]:
        if stream_state:
            stream_state_value = stream_state.get(self.cursor_field)
            if stream_state_value:
                stream_state_value = pendulum.parse(stream_state_value) - self._lookback_window_minutes
                return safe_max(stream_state_value, self._start_date)
        return self._start_date

    def read_records(
        self, stream_slice: Optional[Mapping[str, Any]] = None, stream_state: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        start_point = self.get_starting_point(stream_state=stream_state)
        for record in super().read_records(stream_slice=stream_slice, stream_state=stream_state, **kwargs):
            cursor_value = pendulum.parse(record[self.cursor_field])
            if not start_point or cursor_value >= start_point:
                yield record

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        self._starting_point_cache.clear()
        yield from super().stream_slices(**kwargs)


class ApplicationRoles(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-application-roles/#api-rest-api-3-applicationrole-get
    """

    primary_key = "key"

    def path(self, **kwargs) -> str:
        return "applicationrole"


class Avatars(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-avatars/#api-rest-api-3-avatar-type-system-get
    """

    extract_field = "system"
    avatar_types = ("issuetype", "project", "user")

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"avatar/{stream_slice['avatar_type']}/system"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for avatar_type in self.avatar_types:
            yield {"avatar_type": avatar_type}


class Boards(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/software/rest/api-group-other-operations/#api-agile-1-0-board-get
    """

    extract_field = "values"
    api_v1 = True

    def path(self, **kwargs) -> str:
        return "board"

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        for board in super().read_records(**kwargs):
            location = board.get("location", {})
            if not self._projects or location.get("projectKey") in self._projects:
                yield board

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        location = record.get("location")
        if location:
            record["projectId"] = str(location.get("projectId"))
            record["projectKey"] = location.get("projectKey")
        return record


class BoardIssues(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/software/rest/api-group-board/#api-rest-agile-1-0-board-boardid-issue-get
    """

    cursor_field = "updated"
    extract_field = "issues"
    api_v1 = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._starting_point_cache = {}
        self.boards_stream = Boards(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"board/{stream_slice['board_id']}/issue"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["fields"] = ["key", "created", "updated"]
        jql = self.jql_compare_date(stream_state, stream_slice)
        if jql:
            params["jql"] = jql
        return params

    def jql_compare_date(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any]) -> Optional[str]:
        compare_date = self.get_starting_point(stream_state, stream_slice)
        if compare_date:
            compare_date = compare_date.strftime("%Y/%m/%d %H:%M")
            return f"{self.cursor_field} >= '{compare_date}'"

    def _is_board_error(self, response):
        """Check if board has error and should be skipped"""
        if response.status_code == 500:
            if "This board has no columns with a mapped status." in response.text:
                return True

    def should_retry(self, response: requests.Response) -> bool:
        if self._is_board_error(response):
            return False

        # for all other HTTP errors the default handling is applied
        return super().should_retry(response)

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from read_full_refresh(self.boards_stream)

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            yield from super().read_records(stream_slice={"board_id": stream_slice["id"]}, **kwargs)
        except HTTPError as e:
            if self._is_board_error(e.response):
                # Wrong board is skipped
                self.logger.warning(f"Board {stream_slice['id']} has no columns with a mapped status. Skipping.")
            else:
                raise

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        updated_state = latest_record[self.cursor_field]
        board_id = str(latest_record["boardId"])
        stream_state_value = current_stream_state.get(board_id, {}).get(self.cursor_field)
        if stream_state_value:
            updated_state = max(updated_state, stream_state_value)
        current_stream_state.setdefault(board_id, {})[self.cursor_field] = updated_state
        return current_stream_state

    def get_starting_point(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any]) -> Optional[pendulum.DateTime]:
        board_id = str(stream_slice["board_id"])
        if self.cursor_field not in self._starting_point_cache:
            self._starting_point_cache.setdefault(board_id, {})[self.cursor_field] = self._get_starting_point(
                stream_state=stream_state, stream_slice=stream_slice
            )
        return self._starting_point_cache[board_id][self.cursor_field]

    def _get_starting_point(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any]) -> Optional[pendulum.DateTime]:
        if stream_state:
            board_id = str(stream_slice["board_id"])
            stream_state_value = stream_state.get(board_id, {}).get(self.cursor_field)
            if stream_state_value:
                stream_state_value = pendulum.parse(stream_state_value) - self._lookback_window_minutes
                return safe_max(stream_state_value, self._start_date)
        return self._start_date

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["boardId"] = stream_slice["board_id"]
        record["created"] = record["fields"]["created"]
        record["updated"] = record["fields"]["updated"]
        return record


class Dashboards(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-dashboards/#api-rest-api-3-dashboard-get
    """

    extract_field = "dashboards"

    def path(self, **kwargs) -> str:
        return "dashboard"


class Filters(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-filters/#api-rest-api-3-filter-search-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "filter/search"

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["expand"] = "description,owner,jql,viewUrl,searchUrl,favourite,favouritedCount,sharePermissions,isWritable,subscriptions"
        return params


class FilterSharing(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-filter-sharing/#api-rest-api-3-filter-id-permission-get
    """

    def __init__(self, render_fields: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.filters_stream = Filters(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"filter/{stream_slice['filter_id']}/permission"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for filters in read_full_refresh(self.filters_stream):
            yield from super().read_records(stream_slice={"filter_id": filters["id"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["filterId"] = stream_slice["filter_id"]
        return record


class Groups(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-groups/#api-rest-api-3-group-bulk-get
    """

    extract_field = "values"
    primary_key = "groupId"

    def path(self, **kwargs) -> str:
        return "group/bulk"


class Issues(IncrementalJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-get
    """

    page_size = 100
    state_checkpoint_interval = 10000
    cursor_field = "updated"
    extract_field = "issues"
    _expand_fields_list = ["renderedFields", "transitions", "changelog"]

    # Issue: https://github.com/airbytehq/airbyte/issues/26712
    # we should skip the slice with wrong permissions on project level
    skip_http_status_codes = [requests.codes.FORBIDDEN, requests.codes.BAD_REQUEST]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._project_ids = []
        self.issue_fields_stream = IssueFields(authenticator=self.authenticator, domain=self._domain, projects=self._projects)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, **kwargs) -> str:
        return "search"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["fields"] = "*all"

        jql_parts = [self.jql_compare_date(stream_state)]
        if self._project_ids:
            jql_parts.append(f"project in ({stream_slice.get('project_id')})")
        params["jql"] = " and ".join([p for p in jql_parts if p])
        params["jql"] += f" ORDER BY {self.cursor_field} asc"

        params["expand"] = ",".join(self._expand_fields_list)
        return params

    def transform(self, record: MutableMapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["projectId"] = record["fields"]["project"]["id"]
        record["projectKey"] = record["fields"]["project"]["key"]
        record["created"] = record["fields"]["created"]
        record["updated"] = record["fields"]["updated"]

        # remove fields that are None
        if "renderedFields" in record:
            record["renderedFields"] = {k: v for k, v in record["renderedFields"].items() if v is not None}
        if "fields" in record:
            record["fields"] = {k: v for k, v in record["fields"].items() if v is not None}
        return record

    def get_project_ids(self):
        return [project["id"] for project in read_full_refresh(self.projects_stream)]

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        self._starting_point_cache.clear()
        self._project_ids = []
        if self._projects:
            self._project_ids = self.get_project_ids()
            if not self._project_ids:
                return
            for project_id in self._project_ids:
                yield {"project_id": project_id}
        else:
            yield from super().stream_slices(**kwargs)

    def _get_custom_error(self, response: requests.Response) -> str:
        if response.status_code == requests.codes.BAD_REQUEST:
            return "The user doesn't have permission to the project. Please grant the user to the project."
        return ""


class IssueComments(IncrementalJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
    """

    extract_field = "comments"
    cursor_field = "updated"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/comment"

    def read_records(
        self, stream_slice: Optional[Mapping[str, Any]] = None, stream_state: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        for issue in read_incremental(self.issues_stream, stream_state=stream_state):
            stream_slice = {"key": issue["key"]}
            yield from super().read_records(stream_slice=stream_slice, stream_state=stream_state, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["key"]
        return record


class IssueFields(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-fields/#api-rest-api-3-field-get
    """

    def path(self, **kwargs) -> str:
        return "field"

    def field_ids_by_name(self) -> Mapping[str, List[str]]:
        results = {}
        for f in read_full_refresh(self):
            results.setdefault(f["name"], []).append(f["id"])
        return results


class IssueFieldConfigurations(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-field-configurations/#api-rest-api-3-fieldconfiguration-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "fieldconfiguration"


class IssueCustomFieldContexts(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-rest-api-3-field-fieldid-context-get
    """

    extract_field = "values"
    skip_http_status_codes = [
        # https://community.developer.atlassian.com/t/get-custom-field-contexts-not-found-returned/48408/2
        # /rest/api/3/field/{fieldId}/context - can return 404 if project style is not "classic"
        requests.codes.NOT_FOUND,
        # Only Jira administrators can access custom field contexts.
        requests.codes.FORBIDDEN,
        requests.codes.BAD_REQUEST,
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issue_fields_stream = IssueFields(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"field/{stream_slice['field_id']}/context"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for field in read_full_refresh(self.issue_fields_stream):
            if field.get("custom", False):
                yield from super().read_records(
                    stream_slice={"field_id": field["id"], "field_type": field.get("schema", {}).get("type")}, **kwargs
                )

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["fieldId"] = stream_slice["field_id"]
        record["fieldType"] = stream_slice["field_type"]
        return record


class IssueCustomFieldOptions(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-field-fieldid-context-contextid-option-get
    """

    skip_http_status_codes = [
        requests.codes.NOT_FOUND,
        # Only Jira administrators can access custom field options.
        requests.codes.FORBIDDEN,
        requests.codes.BAD_REQUEST,
    ]

    extract_field = "values"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issue_custom_field_contexts_stream = IssueCustomFieldContexts(
            authenticator=self.authenticator, domain=self._domain, projects=self._projects
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"field/{stream_slice['field_id']}/context/{stream_slice['context_id']}/option"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in read_full_refresh(self.issue_custom_field_contexts_stream):
            if record.get("fieldType") == "option":
                yield from super().read_records(stream_slice={"field_id": record["fieldId"], "context_id": record["id"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["fieldId"] = stream_slice["field_id"]
        record["contextId"] = stream_slice["context_id"]
        return record


class IssueLinkTypes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-link-types/#api-rest-api-3-issuelinktype-get
    """

    extract_field = "issueLinkTypes"

    def path(self, **kwargs) -> str:
        return "issueLinkType"


class IssueNavigatorSettings(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-navigator-settings/#api-rest-api-3-settings-columns-get
    """

    primary_key = "value"

    def path(self, **kwargs) -> str:
        return "settings/columns"


class IssueNotificationSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-notification-schemes/#api-rest-api-3-notificationscheme-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "notificationscheme"


class IssuePriorities(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-priorities/#api-rest-api-3-priority-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "priority/search"


class IssuePropertyKeys(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-properties/#api-rest-api-3-issue-issueidorkey-properties-get
    """

    extract_field = "keys"
    skip_http_status_codes = [
        # Issue does not exist or you do not have permission to see it.
        requests.codes.NOT_FOUND,
        requests.codes.BAD_REQUEST,
    ]

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/properties"

    def read_records(self, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping[str, Any]]:
        issue_key = stream_slice["key"]
        yield from super().read_records(stream_slice={"key": issue_key}, **kwargs)


class IssueProperties(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-properties/#api-rest-api-3-issue-issueidorkey-properties-propertykey-get
    """

    primary_key = "key"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )
        self.issue_property_keys_stream = IssuePropertyKeys(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['issue_key']}/properties/{stream_slice['key']}"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for issue in read_full_refresh(self.issues_stream):
            for property_key in self.issue_property_keys_stream.read_records(stream_slice={"key": issue["key"]}, **kwargs):
                yield from super().read_records(stream_slice={"key": property_key["key"], "issue_key": issue["key"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["issue_key"]
        return record


class IssueRemoteLinks(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-remote-links/#api-rest-api-3-issue-issueidorkey-remotelink-get
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/remotelink"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for issue in read_full_refresh(self.issues_stream):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["key"]
        return record


class IssueResolutions(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-resolutions/#api-rest-api-3-resolution-search-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "resolution/search"


class IssueSecuritySchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-security-schemes/#api-rest-api-3-issuesecurityschemes-get
    """

    extract_field = "issueSecuritySchemes"

    def path(self, **kwargs) -> str:
        return "issuesecurityschemes"


class IssueTypes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-types/#api-group-issue-types
    """

    def path(self, **kwargs) -> str:
        return "issuetype"


class IssueTypeSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-type-schemes/#api-rest-api-3-issuetypescheme-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "issuetypescheme"


class IssueTypeScreenSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-type-screen-schemes/#api-rest-api-3-issuetypescreenscheme-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "issuetypescreenscheme"


class IssueTransitions(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-transitions-get
    """

    primary_key = ["issueId", "id"]
    extract_field = "transitions"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/transitions"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for issue in read_full_refresh(self.issues_stream):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["key"]
        return record


class IssueVotes(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-votes/#api-rest-api-3-issue-issueidorkey-votes-get

    extract_field voters is commented, since it contains the <Users>
    objects but does not contain information about exactly votes. The
    original schema self, votes (number), hasVoted (bool) and list of voters.
    The schema is correct but extract_field should not be applied.
    """

    # extract_field = "voters"
    primary_key = "self"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/votes"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for issue in read_full_refresh(self.issues_stream):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["key"]
        return record


class IssueWatchers(StartDateJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-watchers/#api-rest-api-3-issue-issueidorkey-watchers-get

    extract_field is commented for the same reason as issue_voters.
    """

    # extract_field = "watchers"
    primary_key = "self"
    skip_http_status_codes = [
        # Issue is not found or the user does not have permission to view it.
        requests.codes.NOT_FOUND,
        requests.codes.BAD_REQUEST,
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/watchers"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for issue in read_full_refresh(self.issues_stream):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = stream_slice["key"]
        return record


class IssueWorklogs(IncrementalJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-worklogs/#api-rest-api-3-issue-issueidorkey-worklog-get
    """

    extract_field = "worklogs"
    cursor_field = "updated"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = Issues(
            authenticator=self.authenticator,
            domain=self._domain,
            projects=self._projects,
            start_date=self._start_date,
        )

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"issue/{stream_slice['key']}/worklog"

    def read_records(
        self, stream_slice: Optional[Mapping[str, Any]] = None, stream_state: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        for issue in read_incremental(self.issues_stream, stream_state=stream_state):
            stream_slice = {"key": issue["key"]}
            yield from super().read_records(stream_slice=stream_slice, stream_state=stream_state, **kwargs)


class JiraSettings(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-jira-settings/#api-rest-api-3-application-properties-get
    """

    def path(self, **kwargs) -> str:
        return "application-properties"


class Labels(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-labels/#api-rest-api-3-label-get
    """

    extract_field = "values"
    primary_key = "label"

    def path(self, **kwargs) -> str:
        return "label"

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        return {"label": record}


class Permissions(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-permissions/#api-rest-api-3-permissions-get
    """

    extract_field = "permissions"
    primary_key = "key"

    def path(self, **kwargs) -> str:
        return "permissions"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json.get(self.extract_field, {}).values()
        yield from records


class PermissionSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-permission-schemes/#api-rest-api-3-permissionscheme-get
    """

    extract_field = "permissionSchemes"

    def path(self, **kwargs) -> str:
        return "permissionscheme"


class Projects(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-search-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "project/search"

    def request_params(self, **kwargs):
        params = super().request_params(**kwargs)
        params["expand"] = "description,lead"
        params["status"] = ["live", "archived", "deleted"]
        return params

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in super().read_records(**kwargs):
            if not self._projects or project["key"] in self._projects:
                yield project


class ProjectAvatars(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-avatars/#api-rest-api-3-project-projectidorkey-avatars-get
    """

    skip_http_status_codes = [
        # Project is not found or the user does not have permission to view the project.
        requests.codes.UNAUTHORIZED,
        requests.codes.NOT_FOUND,
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"project/{stream_slice['key']}/avatars"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        stream_slice = kwargs["stream_slice"]
        for records in response_json.values():
            for record in records:
                record["projectId"] = stream_slice["key"]
                yield record

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in read_full_refresh(self.projects_stream):
            # Skip fetching child resources for deleted projects since attempting to do so will return a 404 error.
            if project.get('deleted', None):
                continue

            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class ProjectCategories(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-categories/#api-rest-api-3-projectcategory-get
    """

    skip_http_status_codes = [
        # Project is not found or the user does not have permission to view the project.
        requests.codes.UNAUTHORIZED,
        requests.codes.NOT_FOUND,
    ]

    def path(self, **kwargs) -> str:
        return "projectCategory"


class ProjectComponents(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-components/#api-rest-api-3-project-projectidorkey-component-get
    """

    extract_field = "values"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"project/{stream_slice['key']}/component"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in read_full_refresh(self.projects_stream):
            # Skip fetching child resources for deleted projects since attempting to do so will return a 404 error.
            if project.get('deleted', None):
                continue

            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class ProjectEmail(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-email/#api-rest-api-3-project-projectid-email-get
    """

    primary_key = "projectId"
    skip_http_status_codes = [
        # You cannot edit the configuration of this project.
        requests.codes.FORBIDDEN,
        requests.codes.BAD_REQUEST,
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"project/{stream_slice['project_id']}/email"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in read_full_refresh(self.projects_stream):
            # Skip fetching emails for deleted projects since attempting to do so will return a 404 error.
            if project.get('deleted', None):
                continue

            yield from super().read_records(stream_slice={"project_id": project["id"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["projectId"] = stream_slice["project_id"]
        return record


class ProjectPermissionSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-permission-schemes/#api-rest-api-3-project-projectkeyorid-securitylevel-get
    """

    extract_field = "levels"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"project/{stream_slice['key']}/securitylevel"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in read_full_refresh(self.projects_stream):
            # Skip fetching child resources for deleted projects since attempting to do so will return a 404 error.
            if project.get('deleted', None):
                continue

            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["projectId"] = stream_slice["key"]
        return record


class ProjectRoles(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-roles#api-rest-api-3-role-get
    """

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "role"


class ProjectTypes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-types/#api-rest-api-3-project-type-get
    """

    primary_key = "key"

    def path(self, **kwargs) -> str:
        return "project/type"

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in super().read_records(**kwargs):
            if project.get("descriptionI18nKey"):
                project["descriptionKey"] = deepcopy(project["descriptionI18nKey"])
                del project["descriptionI18nKey"]
                yield project


class ProjectVersions(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-versions/#api-rest-api-3-project-projectidorkey-version-get
    """

    extract_field = "values"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects_stream = Projects(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"project/{stream_slice['key']}/version"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for project in read_full_refresh(self.projects_stream):
            # Skip fetching child resources for deleted projects since attempting to do so will return a 404 error.
            if project.get('deleted', None):
                continue

            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class PullRequests(IncrementalJiraStream):
    """
    This stream uses an undocumented internal API endpoint used by the Jira
    webapp. Jira does not publish any specifications about this endpoint, so the
    only way to get details about it is to use a web browser, view a Jira issue
    that has a linked pull request, and inspect the network requests using the
    browser's developer console.
    """

    cursor_field = "updated"
    extract_field = "detail"
    raise_on_http_errors = False

    pr_regex = r"(?P<prDetails>PullRequestOverallDetails{openCount=(?P<open>[0-9]+), mergedCount=(?P<merged>[0-9]+), declinedCount=(?P<declined>[0-9]+)})|(?P<pr>pullrequest={dataType=pullrequest, state=(?P<state>[a-zA-Z]+), stateCount=(?P<count>[0-9]+)})"

    def __init__(self, issues_stream: Issues, issue_fields_stream: IssueFields, **kwargs):
        super().__init__(**kwargs)
        self.issues_stream = issues_stream
        self.issue_fields_stream = issue_fields_stream

    @property
    def url_base(self) -> str:
        return f"https://{self._domain}/rest/dev-status/1.0/"

    def path(self, **kwargs) -> str:
        return "issue/detail"

    # Currently, only GitHub pull requests are supported by this stream. The
    # requirements for supporting other systems are unclear.
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_slice=stream_slice, **kwargs)
        params["issueId"] = stream_slice["id"]
        params["applicationType"] = "GitHub"
        params["dataType"] = "branch"
        return params

    def has_pull_requests(self, dev_field) -> bool:
        if not dev_field or dev_field == "{}":
            return False
        matches = 0
        for match in re.finditer(self.pr_regex, dev_field, re.MULTILINE):
            if match.group("prDetails"):
                matches += int(match.group("open")) + int(match.group("merged")) + int(match.group("declined"))
            elif match.group("pr"):
                matches += int(match.group("count"))
        return matches > 0

    def read_records(
        self, stream_slice: Optional[Mapping[str, Any]] = None, stream_state: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        field_ids_by_name = self.issue_fields_stream.field_ids_by_name()
        dev_field_ids = field_ids_by_name.get("Development", [])
        for issue in read_incremental(self.issues_stream, stream_state=stream_state):
            for dev_field_id in dev_field_ids:
                if self.has_pull_requests(issue["fields"].get(dev_field_id)):
                    yield from super().read_records(
                        stream_slice={"id": issue["id"], self.cursor_field: issue["fields"][self.cursor_field]}, **kwargs
                    )
                    break

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["id"] = stream_slice["id"]
        record[self.cursor_field] = stream_slice[self.cursor_field]
        return record


class Screens(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screens/#api-rest-api-3-screens-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "screens"


class ScreenTabs(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-tabs/#api-rest-api-3-screens-screenid-tabs-get
    """

    raise_on_http_errors = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.screens_stream = Screens(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"screens/{stream_slice['screen_id']}/tabs"

    # This endpoint doesn't have pagination, so we override the pagination strategy defined in JiraStream to avoid
    # getting stuck in a loop making the same request endlessly.
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for screen in read_full_refresh(self.screens_stream):
            yield from self.read_tab_records(stream_slice={"screen_id": screen["id"]}, **kwargs)

    def read_tab_records(self, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping[str, Any]]:
        screen_id = stream_slice["screen_id"]
        for screen_tab in super().read_records(stream_slice={"screen_id": screen_id}, **kwargs):
            """
            For some projects jira creates screens automatically, which does not present in UI, but exist in screens stream.
            We receive 400 error "Screen with id {screen_id} does not exist" for tabs by these screens.
            """
            bad_request_reached = re.match(r"Screen with id \d* does not exist", screen_tab.get("errorMessages", [""])[0])
            if bad_request_reached:
                self.logger.info("Could not get screen tab for %s screen id. Reason: %s", screen_id, screen_tab["errorMessages"][0])
                return
            yield screen_tab

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["screenId"] = stream_slice["screen_id"]
        return record


class ScreenTabFields(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-tab-fields/#api-rest-api-3-screens-screenid-tabs-tabid-fields-get
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.screens_stream = Screens(authenticator=self.authenticator, domain=self._domain, projects=self._projects)
        self.screen_tabs_stream = ScreenTabs(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"screens/{stream_slice['screen_id']}/tabs/{stream_slice['tab_id']}/fields"

    # This endpoint doesn't have pagination, so we override the pagination strategy defined in JiraStream to avoid
    # getting stuck in a loop making the same request endlessly.
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for screen in read_full_refresh(self.screens_stream):
            for tab in self.screen_tabs_stream.read_tab_records(stream_slice={"screen_id": screen["id"]}, **kwargs):
                if "id" in tab:  # Check for proper tab record since the ScreenTabs stream doesn't throw http errors
                    yield from super().read_records(stream_slice={"screen_id": screen["id"], "tab_id": tab["id"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["screenId"] = stream_slice["screen_id"]
        record["tabId"] = stream_slice["tab_id"]
        return record


class ScreenSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-schemes/#api-rest-api-3-screenscheme-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "screenscheme"


class Sprints(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/software/rest/api-group-board/#api-rest-agile-1-0-board-boardid-sprint-get
    """

    extract_field = "values"
    api_v1 = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.boards_stream = Boards(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def _get_custom_error(self, response: requests.Response) -> str:
        if response.status_code == requests.codes.BAD_REQUEST:
            errors = response.json().get("errorMessages")
            for error_message in errors:
                if "The board does not support sprints" in error_message:
                    return (
                        "The board does not support sprints. The board does not have a sprint board. if it's a team-managed one, "
                        "does it have sprints enabled under project settings? If it's a company-managed one,"
                        " check that it has at least one Scrum board associated with it."
                    )
        return ""

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"board/{stream_slice['board_id']}/sprint"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        available_board_types = ["scrum", "simple"]
        for board in read_full_refresh(self.boards_stream):
            if board["type"] in available_board_types:
                board_details = {"name": board["name"], "id": board["id"]}
                self.logger.info(f"Fetching sprints for board: {board_details}")
                yield from super().read_records(stream_slice={"board_id": board["id"]}, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["boardId"] = stream_slice["board_id"]
        return record


class SprintIssues(IncrementalJiraStream):
    """
    https://developer.atlassian.com/cloud/jira/software/rest/api-group-sprint/#api-rest-agile-1-0-sprint-sprintid-issue-get
    """

    cursor_field = "updated"
    extract_field = "issues"
    api_v1 = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sprints_stream = Sprints(authenticator=self.authenticator, domain=self._domain, projects=self._projects)
        self.issue_fields_stream = IssueFields(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return f"sprint/{stream_slice['sprint_id']}/issue"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["fields"] = stream_slice["fields"]
        jql = self.jql_compare_date(stream_state)
        if jql:
            params["jql"] = jql
        return params

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        fields = self.get_fields()
        for sprint in read_full_refresh(self.sprints_stream):
            stream_slice = {"sprint_id": sprint["id"], "fields": fields}
            yield from super().read_records(stream_slice=stream_slice, **kwargs)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        record["issueId"] = record["id"]
        record["id"] = "-".join([str(stream_slice["sprint_id"]), record["id"]])
        record["sprintId"] = stream_slice["sprint_id"]
        record["created"] = record["fields"]["created"]
        record["updated"] = record["fields"]["updated"]
        return record

    def get_fields(self):
        fields = ["key", "status", "created", "updated"]
        field_ids_by_name = self.issue_fields_stream.field_ids_by_name()
        for name in ["Story Points", "Story point estimate"]:
            if name in field_ids_by_name:
                fields.extend(field_ids_by_name[name])
        return fields


class TimeTracking(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-time-tracking/#api-rest-api-3-configuration-timetracking-list-get
    """

    primary_key = "key"

    def path(self, **kwargs) -> str:
        return "configuration/timetracking/list"


class Users(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-users-search-get
    """

    primary_key = "accountId"

    def path(self, **kwargs) -> str:
        return "users/search"


class UsersGroupsDetailed(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
    """

    primary_key = "accountId"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.users_stream = Users(authenticator=self.authenticator, domain=self._domain, projects=self._projects)

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        return "user"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["accountId"] = stream_slice["accountId"]
        params["expand"] = "groups,applicationRoles"
        return params

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for user in read_full_refresh(self.users_stream):
            yield from super().read_records(stream_slice={"accountId": user["accountId"]}, **kwargs)


class Workflows(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflows/#api-rest-api-3-workflow-search-get
    """

    extract_field = "values"
    primary_key = "entity_id"

    def path(self, **kwargs) -> str:
        return "workflow/search"

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            for record in super().read_records(**kwargs):
                record["entity_id"] = record["id"]["entityId"]
                yield record
        except HTTPError as e:
            if not (self.skip_http_status_codes and e.response.status_code in self.skip_http_status_codes):
                raise e
            errors = e.response.json().get("errorMessages")
            custom_error = self._get_custom_error(e.response)
            self.logger.warning(f"Stream `{self.name}`. An error occurred, details: {errors}. Skipping for now. {custom_error}")


class WorkflowSchemes(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-schemes/#api-rest-api-3-workflowscheme-get
    """

    extract_field = "values"

    def path(self, **kwargs) -> str:
        return "workflowscheme"


class WorkflowStatuses(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-statuses/#api-rest-api-3-status-get
    """

    def path(self, **kwargs) -> str:
        return "status"


class WorkflowStatusCategories(JiraStream):
    """
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-status-categories/#api-rest-api-3-statuscategory-get
    """

    def path(self, **kwargs) -> str:
        return "statuscategory"
