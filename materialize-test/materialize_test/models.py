from pydantic import BaseModel, Field


class EndpointConfig(BaseModel):
    pass


class ConnectorState(BaseModel):
    last_runtime_cp_json: str | None = None


class ResourceConfig(BaseModel):
    path: str = Field(json_schema_extra={"x-collection-name": True})
