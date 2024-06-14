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
    id: int = Field(description="The item's unique id.")
    deleted: bool = Field(False, description="true if the item is deleted.")
    type: str = Field(description='The type of item. One of "job", "story", "comment", "poll", or "pollopt".')
    by: Optional[str] = Field(None, description="The username of the item's author.")
    time: datetime = Field(description="Creation date of the item, in Unix Time.")
    text: Optional[str] = Field(None, description="The comment, story or poll text. HTML.")
    dead: bool = Field(False, description="true if the item is dead.")
    parent: Optional[int] = Field(None, description="The comment's parent: either another comment or the relevant story.")
    poll: Optional[int] = Field(None, description="The pollopt's associated poll.")
    kids: List[int] = Field(default_factory=list, description="The ids of the item's comments, in ranked display order.")
    url: Optional[HttpUrl] = Field(None, description="The URL of the story.")
    score: Optional[int] = Field(None, description="The story's score, or the votes for a pollopt.")
    title: Optional[str] = Field(None, description="The title of the story, poll or job. HTML.")
    parts: Optional[List[int]] = Field(None, description="A list of related pollopts, in display order.")
    descendants: Optional[int] = Field(None, description="In the case of stories or polls, the total comment count.")
