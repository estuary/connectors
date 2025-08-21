from datetime import datetime, timedelta, UTC
from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import Optional, List, ClassVar
import html

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceState,
    BaseDocument,
    ResourceConfig as BaseResourceConfig,
)


class EndpointConfig(BaseModel):
    """
    HackerNews API endpoint configuration.
    HackerNews API requires no authentication - it's completely public.
    """

    # No configuration needed for HackerNews API
    pass


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]


class ResourceConfig(BaseResourceConfig):
    """
    Configuration for HackerNews resources.
    """

    name: str = Field(description="Resource name")
    interval: timedelta = Field(
        default_factory=lambda: timedelta(minutes=5),
        description="Polling interval for incremental updates",
    )

    # Required PATH_POINTERS for resource identification
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]


class Item(BaseDocument):
    """
    HackerNews Item model representing stories, comments, jobs, polls, and poll options.
    All timestamps are converted from Unix time to datetime objects.
    """

    id: int = Field(description="The item's unique id.")
    deleted: bool = Field(False, description="true if the item is deleted.")
    type: str = Field(
        description='The type of item. One of "job", "story", "comment", "poll", or "pollopt".'
    )
    by: Optional[str] = Field(None, description="The username of the item's author.")
    time: datetime = Field(
        description="Creation date of the item, converted from Unix timestamp."
    )
    text: Optional[str] = Field(
        None, description="The comment, story or poll text. HTML."
    )
    dead: bool = Field(False, description="true if the item is dead.")
    parent: Optional[int] = Field(
        None,
        description="The comment's parent: either another comment or the relevant story.",
    )
    poll: Optional[int] = Field(None, description="The pollopt's associated poll.")
    kids: List[int] = Field(
        default_factory=list,
        description="The ids of the item's comments, in ranked display order.",
    )
    url: Optional[HttpUrl] = Field(None, description="The URL of the story.")
    score: Optional[int] = Field(
        None, description="The story's score, or the votes for a pollopt."
    )
    title: Optional[str] = Field(
        None, description="The title of the story, poll or job. HTML."
    )
    parts: Optional[List[int]] = Field(
        None, description="A list of related pollopts, in display order."
    )
    descendants: Optional[int] = Field(
        None, description="In the case of stories or polls, the total comment count."
    )

    @field_validator("time", mode="before")
    @classmethod
    def convert_unix_timestamp(cls, v) -> datetime:
        """Convert Unix timestamp to datetime object with UTC timezone."""
        if isinstance(v, int):
            return datetime.fromtimestamp(v, tz=UTC)
        elif isinstance(v, datetime):
            return v
        else:
            raise ValueError(f"Invalid time value: {v}")

    @field_validator("url", mode="before")
    @classmethod
    def decode_html_entities(cls, v) -> Optional[str]:
        """Decode HTML entities in URLs before validation."""
        if v is None:
            return None
        if isinstance(v, str):
            # Decode HTML entities like &#38; to &
            return html.unescape(v)
        return v

    @field_validator("url", mode="before")
    @classmethod
    def clean_url(cls, v: str | None) -> str | None:
        """Clean URLs by removing trailing artifacts commonly found in HackerNews data."""
        if v is None:
            return v
        if isinstance(v, str):
            # Strip trailing pipes, spaces, and other common URL artifacts from HackerNews
            cleaned = v.rstrip("| \t\n\r")
            # Handle cases where the URL might be completely invalid
            if not cleaned or cleaned.isspace():
                return None
            return cleaned
        return v


class User(BaseDocument):
    """
    HackerNews User model representing user profiles and metadata.
    """

    id: str = Field(description="The user's unique username.")
    created: datetime = Field(
        description="Creation date of the user account, converted from Unix timestamp."
    )
    karma: int = Field(description="The user's karma score.")
    about: Optional[str] = Field(
        None, description="The user's optional self-description. HTML."
    )
    submitted: List[int] = Field(
        default_factory=list, description="List of the user's submitted item IDs."
    )

    @field_validator("created", mode="before")
    @classmethod
    def convert_unix_timestamp(cls, v) -> datetime:
        """Convert Unix timestamp to datetime object with UTC timezone."""
        if isinstance(v, int):
            return datetime.fromtimestamp(v, tz=UTC)
        elif isinstance(v, datetime):
            return v
        else:
            raise ValueError(f"Invalid created value: {v}")


class UpdatedItems(BaseDocument):
    """
    HackerNews Updates model representing changed items and profiles.
    """

    items: List[int] = Field(
        default_factory=list, description="List of updated item IDs."
    )
    profiles: List[str] = Field(
        default_factory=list, description="List of updated user profiles."
    )
