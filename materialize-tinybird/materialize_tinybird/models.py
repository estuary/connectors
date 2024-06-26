from pydantic import BaseModel, Field


class EndpointConfig(BaseModel):
    token: str = Field(
        title="Auth Token",
        description="Tinybird Auth Token",
    )


class ConnectorState(BaseModel):
    pass


class ResourceConfig(BaseModel):
    name: str = Field(
        title="Data Source Name",
        description="Name of the target Data Source",
        json_schema_extra={"x-collection-name": True},
    )


class EventsApiResponse(BaseModel, extra="forbid"):
    successful_rows: int
    quarantined_rows: int