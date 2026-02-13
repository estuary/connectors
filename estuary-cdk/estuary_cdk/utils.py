import asyncio
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass
class TaskInfo:
    """Information about an asyncio task."""
    task: asyncio.Task[Any]
    task_name: str
    coro_name: str
    stack_trace: str


def get_running_tasks_info(
    exclude_tasks: set[asyncio.Task[Any]] | None = None,
) -> list[TaskInfo]:
    """
    Returns information about all currently running asyncio tasks,
    excluding any tasks in exclude_tasks and any completed tasks.
    """
    loop = asyncio.get_running_loop()
    all_tasks = asyncio.all_tasks(loop)
    exclude = exclude_tasks or set()

    result = []
    for task in all_tasks:
        if task in exclude or task.done():
            continue

        task_name = task.get_name()
        coro = task.get_coro()
        coro_name = coro.__name__ if coro else "unknown"

        frames = task.get_stack()
        if frames:
            summary = traceback.StackSummary.extract(
                (frame, frame.f_lineno) for frame in frames
            )
            stack_trace = "".join(summary.format()).rstrip()
        else:
            stack_trace = "No stack available"

        result.append(TaskInfo(
            task=task,
            task_name=task_name,
            coro_name=coro_name,
            stack_trace=stack_trace,
        ))

    return result


def format_error_message(err: BaseException):
    msg = f"{err}"
    # If the exeption doesn't have a meaningful string representation,
    # set it to the exception type's name so something more useful is logged.
    if msg == "":
        msg = f"{type(err).__name__}"

    return msg


def sort_dict(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: sort_dict(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [sort_dict(item) for item in obj]
    return obj


def json_merge_patch(target: dict, patch: dict) -> None:
    """
    Apply JSON Merge Patch (RFC 7396) to target dictionary in-place.

    Args:
        target: The dictionary to be modified
        patch: The patch to apply

    Semantics:
        - null values remove keys from target
        - nested objects are recursively merged
        - arrays and primitives are replaced entirely
    """

    for key, value in patch.items():
        if value is None:
            # Remove the key if value is None
            target.pop(key, None)
        elif (
            isinstance(value, dict) and key in target and isinstance(target[key], dict)
        ):
            # Recursively merge dictionaries
            json_merge_patch(target[key], value)
        else:
            # Replace the value (including arrays)
            target[key] = value
