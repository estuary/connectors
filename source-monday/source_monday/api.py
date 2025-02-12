import json
from typing import AsyncGenerator
from logging import Logger

from estuary_cdk.http import HTTPSession
from .models import Board

API = "https://api.monday.com/v2"


async def snapshot_boards(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Board, None]:
    # TODO(justin): complete this function
    response = await http.request(
        log,
        API,
        method="POST",
        json={"query": "query {boards {id name}}"},
    )
    data = json.loads(response)

    for board in data["data"]["boards"]:
        yield Board.model_validate(board)
