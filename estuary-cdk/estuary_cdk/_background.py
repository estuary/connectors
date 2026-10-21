"""Fire-and-forget task spawning that keeps a strong reference.

asyncio only holds a weak reference to a running task, so a task whose only
reference is the `create_task()` return value can be garbage-collected
mid-flight. Callers here want to start work without awaiting it, so the
reference has to live somewhere until the task finishes.
"""

import asyncio
from typing import Any, Coroutine

_tasks: set[asyncio.Task[Any]] = set()


def spawn(coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task[Any]:
    """Start `coro` as a task which is not awaited, and keep it alive."""
    task = asyncio.create_task(coro, name=name)
    _tasks.add(task)
    _ = task.add_done_callback(_tasks.discard)
    return task
