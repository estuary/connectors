from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    ZendeskResource,
    TimestampedResource,
    IncrementalCursorPaginatedResponse,
    Post,
    PostsResponse,
    PostComment,
    PostCommentsResponse,
    PostCommentVotesResponse,
)

from .snapshots import (
    snapshot_cursor_paginated_resources,
)

from .client_side_incremental import (
    fetch_client_side_incremental_cursor_paginated_resources,
)


async def fetch_post_child_resources(
    http: HTTPSession,
    subdomain: str,
    path_segment: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    posts_generator = fetch_client_side_incremental_cursor_paginated_resources(http, subdomain, "community/posts", None, PostsResponse, log, log_cursor)

    async for result in posts_generator:
        if isinstance(result, TimestampedResource):
            post = Post.model_validate(result)

            if (
                (path_segment == "votes" and post.vote_count == 0) or 
                (path_segment == "comments" and post.comment_count == 0)
            ):
                continue

            path = f"community/posts/{post.id}/{path_segment}"

            async for child_resource in snapshot_cursor_paginated_resources(http, subdomain, path, response_model, log):
                yield ZendeskResource.model_validate({
                    "post_id": post.id,
                    **child_resource.model_dump(exclude={"meta_"}),
                })

        else:
            yield result


async def fetch_post_comment_votes(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    post_comments_generator = fetch_post_child_resources(http, subdomain, "comments", PostCommentsResponse, log, log_cursor)

    async for result in post_comments_generator:

        if isinstance(result, ZendeskResource):
            post_comment = PostComment.model_validate(result.model_dump())  

            if post_comment.vote_count == 0:
                continue

            path = f"community/posts/{post_comment.post_id}/comments/{post_comment.id}/votes"

            async for child_resource in snapshot_cursor_paginated_resources(http, subdomain, path, PostCommentVotesResponse, log):
                yield ZendeskResource.model_validate({
                    "post_id": post_comment.post_id,
                    **child_resource.model_dump(exclude={"meta_"}),
                })

        else:
            yield result
