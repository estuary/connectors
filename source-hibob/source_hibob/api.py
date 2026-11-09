import itertools
from collections import Counter
from collections.abc import AsyncGenerator
from logging import Logger
from typing import Any

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import TypeAdapter

from .models import (
    BulkTableEntry,
    BulkTableRemainder,
    BulkTableResult,
    Employee,
    EmployeeField,
    EmployeeIdsResponse,
    EmployeesResponse,
    NamedList,
)

API = "https://api.hibob.com/v1"
SANDBOX_API = "https://api.sandbox.hibob.com/v1"

# People search rejects requests for more than 400 fields.
MAX_FIELDS_PER_SEARCH = 400
EMPLOYEE_ID_BATCH_SIZE = 100
# The bulk tables reject a `limit` over 200.
BULK_TABLE_PAGE_LIMIT = 200

# Fields whose values change without any HR change (signed media URLs, and tenure / age
# counters that tick daily). Capturing them would re-emit every employee on every poll.
VOLATILE_FIELDS = {
    "root.avatarUrl",
    "root.coverImageUrl",
    "about.avatar",
    "work.tenureDuration",
    "work.durationOfEmployment",
    "work.yearsOfService",
    "work.tenureDurationYears",
    "work.tenureYears",
    "personal.age",
    "payroll.timeSinceLastSalaryChange",
    "internal.periodSinceTermination",
    "internal.yearsSinceTermination",
}
# Categories HiBob recomputes on its own schedule (People Analytics attrition-risk scores).
VOLATILE_CATEGORIES = {"peopleAnalytics"}

_employee_fields_adapter = TypeAdapter(list[EmployeeField])
_named_lists_adapter = TypeAdapter(dict[str, NamedList])


async def list_employee_ids(http: HTTPSession, base_url: str, log: Logger) -> list[str]:
    # Terminated employees are hidden unless `showInactive` is set.
    response = await http.request(
        log,
        f"{base_url}/people/search",
        method="POST",
        json={"fields": ["root.id"], "showInactive": True},
    )
    return sorted(
        e.id for e in EmployeeIdsResponse.model_validate_json(response).employees
    )


async def _fetch_employee_fields(
    http: HTTPSession, base_url: str, log: Logger
) -> list[EmployeeField]:
    response = await http.request(log, f"{base_url}/company/people/fields")
    return sorted(_employee_fields_adapter.validate_json(response), key=lambda f: f.id)


async def snapshot_employee_fields(
    http: HTTPSession, base_url: str, log: Logger
) -> AsyncGenerator[EmployeeField, None]:
    for field in await _fetch_employee_fields(http, base_url, log):
        yield field


async def snapshot_named_lists(
    http: HTTPSession, base_url: str, log: Logger
) -> AsyncGenerator[NamedList, None]:
    response = await http.request(
        log,
        f"{base_url}/company/named-lists",
        params={"includeArchived": "true"},
    )
    lists = _named_lists_adapter.validate_json(response)
    for name in sorted(lists):
        yield lists[name]


def _deep_merge(into: dict[str, Any], other: dict[str, Any]) -> None:
    for key, value in other.items():
        existing = into.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            _deep_merge(existing, value)
        else:
            into[key] = value


async def snapshot_employees(
    http: HTTPSession, base_url: str, log: Logger
) -> AsyncGenerator[Employee, None]:
    # Request every field the service user can see, so custom fields are picked up as
    # they're added. Fields it isn't permitted to view are silently omitted by HiBob.
    field_ids = sorted(
        {
            f.id
            for f in await _fetch_employee_fields(http, base_url, log)
            if f.id.split(".", 1)[0] not in VOLATILE_CATEGORIES
        }
        - VOLATILE_FIELDS
        - {"root.id"}
    )
    field_slices = [
        ["root.id", *chunk]
        for chunk in itertools.batched(
            field_ids, MAX_FIELDS_PER_SEARCH - 1, strict=False
        )
    ] or [["root.id"]]

    employee_ids = await list_employee_ids(http, base_url, log)

    for batch in itertools.batched(employee_ids, EMPLOYEE_ID_BATCH_SIZE, strict=False):
        merged: dict[str, dict[str, Any]] = {}
        # Number of field slices each employee was returned in.
        seen: Counter[str] = Counter()

        for fields in field_slices:
            response = await http.request(
                log,
                f"{base_url}/people/search",
                method="POST",
                json={
                    "fields": fields,
                    "filters": [
                        {
                            "fieldPath": "root.id",
                            "operator": "equals",
                            "values": list(batch),
                        }
                    ],
                    "showInactive": True,
                },
            )

            for employee in EmployeesResponse.model_validate_json(response).employees:
                # Each field is returned twice: nested under its category, and as a
                # top-level "/category/field" key that omits nulls. Keep the nested form.
                nested = {k: v for k, v in employee.items() if not k.startswith("/")}
                _deep_merge(merged.setdefault(nested["id"], {}), nested)
                seen[nested["id"]] += 1

        # An employee missing from any slice would be emitted with only some of its
        # fields, so it's skipped like one deleted between listing ids and reading them.
        if missing := [i for i in batch if seen[i] < len(field_slices)]:
            log.warning(
                "employees listed but not returned by every search, skipping",
                {"missing_employee_ids": missing},
            )
        for employee_id in batch:
            if seen[employee_id] == len(field_slices):
                yield Employee.model_validate(merged[employee_id])


async def snapshot_bulk_table(
    http: HTTPSession,
    base_url: str,
    table: type[BulkTableEntry],
    log: Logger,
) -> AsyncGenerator[BulkTableEntry, None]:
    url = f"{base_url}/bulk/people/{table.path}"
    params: dict[str, str | int] = {
        "limit": BULK_TABLE_PAGE_LIMIT,
        "includeArchived": "true",
    }
    restricted: set[str] = set()
    # Buffered so the snapshot is emitted in a stable order: row_ids are positional and the
    # API's result order isn't guaranteed across polls.
    entries: list[BulkTableEntry] = []

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            "results.item",
            BulkTableResult,
            remainder_cls=BulkTableRemainder,
        )

        async for result in processor:
            for entry in result.values:
                entries.append(
                    table.model_validate({"employeeId": result.employeeId, **entry})
                )

        remainder = processor.get_remainder()
        # Emitting the snapshot without these employees would delete their history
        # downstream, so fail the poll instead.
        if remainder.errors:
            raise RuntimeError(
                f"{table.name}: HiBob reported per-employee errors: {remainder.errors}"
            )
        if remainder.restricted_columns:
            restricted |= remainder.restricted_columns.all()

        if remainder.response_metadata.next_cursor is None:
            break
        params["cursor"] = remainder.response_metadata.next_cursor

    if restricted:
        log.warning(
            f"{table.name}: the service user lacks View or View history permission on some columns, so they are omitted or null",
            {"restricted_columns": sorted(restricted)},
        )

    entries.sort(key=lambda e: (e.employeeId, e.id))
    for entry in entries:
        yield entry
