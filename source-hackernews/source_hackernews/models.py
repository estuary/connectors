from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceState, BaseDocument, ResourceConfig as BaseResourceConfig,
)


class EndpointConfig(BaseModel):
    pass


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]


class ResourceConfig(BaseResourceConfig):
    pass


class Item(BaseDocument):
    """
    Hackernews Item
    """
    id: int
    deleted: bool = False
    type: Optional[str] = Field(None, description='One of "job", "story", "comment", "poll", or "pollopt"')
    by: Optional[str] = None
    time: Optional[datetime] = Field(None, description='Unix Time')
    text: Optional[str] = Field(None, description='HTML')
    dead: bool = False
    parent: Optional[int] = Field(None, description="ID of the parent item")
    poll: Optional[int] = Field(None, description="ID of the associated poll for pollopt")
    kids: List[int] = Field(default_factory=list, description="List of IDs of comments")
    url: Optional[HttpUrl] = None
    score: Optional[int] = None
    title: Optional[str] = Field(None, description='HTML')
    parts: List[int] = Field(default_factory=list, description="List of related pollopts")
    descendants: Optional[int] = Field(None, description="Total comment count for stories or polls")
