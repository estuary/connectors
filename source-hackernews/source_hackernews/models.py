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
    Base Hackernews Item model with common fields
    """
    id: Optional[int] = None
    deleted: bool = False
    type: Optional[str] = Field(None, description='One of "job", "story", "comment", "poll", or "pollopt"')
    by: Optional[str] = None
    time: Optional[datetime] = Field(None, description='Unix Time')
    dead: bool = False
    kids: List[int] = Field(default_factory=list, description="List of IDs of comments")


class Story(Item):
    """
    Hackernews Story or Ask HN
    """
    title: Optional[str] = Field(None, description='HTML')
    url: Optional[HttpUrl] = None
    score: Optional[int] = None
    descendants: Optional[int] = Field(None, description="Total comment count")
    text: Optional[str] = Field(None, description='HTML')


class Comment(Item):
    """
    Hackernews Comment
    """
    parent: int = Field(..., description="ID of the parent item")
    text: str = Field(..., description='HTML')


class Job(Item):
    """
    Hackernews Job Posting
    """
    title: str = Field(..., description='HTML')
    text: str = Field(..., description='HTML')
    url: Optional[HttpUrl] = None
    score: Optional[int] = None


class Poll(Item):
    """
    Hackernews Poll
    """
    title: str = Field(..., description='HTML')
    text: Optional[str] = Field(None, description='HTML')
    score: int
    descendants: int = Field(..., description="Total comment count")
    parts: List[int] = Field(..., description="List of related pollopts")


class PollOption(Item):
    """
    Hackernews Poll Option
    """
    poll: int = Field(..., description="ID of the associated poll")
    text: str = Field(..., description='HTML')
    score: int


class User(BaseDocument):
    """
    Hackernews User
    """
    id: str
    created: datetime
    karma: int
    about: Optional[str] = Field(None, description='HTML')
    submitted: List[int] = Field(default_factory=list, description="List of the user's stories, polls and comments")

