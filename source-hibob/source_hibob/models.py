from typing import Any, ClassVar, Literal

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import BasicAuth
from pydantic import BaseModel, Field


class ServiceUser(BasicAuth):
    credentials_title: Literal["Service User"] = Field(
        default="Service User",
        json_schema_extra={"type": "string", "order": 0},
    )
    username: str = Field(
        title="Service User ID",
        description="The ID of the HiBob service user.",
        json_schema_extra={"order": 1},
    )
    password: str = Field(
        title="Service User Token",
        description="The token generated for the HiBob service user.",
        json_schema_extra={"secret": True, "order": 2},
    )


class Advanced(BaseModel):
    sandbox: bool = Field(
        default=False,
        title="Sandbox",
        description="Capture from the HiBob sandbox environment (api.sandbox.hibob.com) instead of production.",
    )


class EndpointConfig(BaseModel):
    credentials: ServiceUser = Field(
        discriminator="credentials_title",
        title="Authentication",
        description=(
            "A HiBob service user. Its permission group must grant View (and View history) on the "
            "People's data categories to capture, and its 'Access data for' scope must not be "
            "limited to employed people, or terminated employees are silently omitted."
        ),
    )
    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class EmployeeField(BaseDocument, extra="allow"):
    id: str
    jsonPath: str


class NamedListItem(BaseModel, extra="allow"):
    # Item ids are list-dependent: `site` items have int ids, `department` items have str ids.
    id: int | str
    children: list["NamedListItem"] = []


class NamedList(BaseDocument, extra="allow"):
    name: str
    items: list[NamedListItem]
    # HiBob returns every list's items twice, under `items` and `values`.
    values: list[Any] = Field(default=[], exclude=True)


class Employee(BaseDocument, extra="allow"):
    id: str


class EmployeeId(BaseModel, extra="allow"):
    id: str


class EmployeeIdsResponse(BaseModel, extra="allow"):
    employees: list[EmployeeId]


class EmployeesResponse(BaseModel, extra="allow"):
    employees: list[dict[str, Any]]


class BulkTableEntry(BaseDocument, extra="allow"):
    """One row of a HiBob historical employee table, stamped with its employee."""

    name: ClassVar[str]
    path: ClassVar[str]

    # Entries don't carry their employee's id; it's stamped from the enclosing result.
    employeeId: str
    # Entry ids are only unique per employee, so the natural key is (employeeId, id).
    id: int


class EmploymentHistory(BulkTableEntry):
    name: ClassVar[str] = "employment_history"
    path: ClassVar[str] = "employment"


class LifecycleHistory(BulkTableEntry):
    name: ClassVar[str] = "lifecycle_history"
    path: ClassVar[str] = "lifecycle"


class WorkHistory(BulkTableEntry):
    name: ClassVar[str] = "work_history"
    path: ClassVar[str] = "work"


BULK_TABLE_STREAMS: list[type[BulkTableEntry]] = [
    EmploymentHistory,
    LifecycleHistory,
    WorkHistory,
]


class BulkTableResult(BaseModel, extra="allow"):
    employeeId: str
    values: list[dict[str, Any]]


class RestrictedColumns(BaseModel, extra="allow"):
    no_view_permission: list[str] = []
    no_view_history_permission: list[str] = []

    def all(self) -> set[str]:
        return {*self.no_view_permission, *self.no_view_history_permission}


class BulkTableRemainder(BaseModel, extra="allow"):
    class ResponseMetadata(BaseModel, extra="allow"):
        next_cursor: str | None = None

    response_metadata: ResponseMetadata
    # Keyed by employee id, despite the docs describing an array.
    errors: dict[str, Any] = {}
    restricted_columns: RestrictedColumns | None = None
