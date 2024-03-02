from dataclasses import dataclass
from logging import Logger
from pydantic import BaseModel
from typing import TypeVar, Callable, AsyncGenerator, Generic
import abc
import asyncio
import signal
import sys
import traceback

from .logger import init_logger
from .flow import ValidationError

# Request type served by this connector.
Request = TypeVar("Request", bound=BaseModel)


# stdin_jsonl parses newline-delimited JSON instances of `cls` from stdin and yields them.
async def stdin_jsonl(cls: type[Request]) -> AsyncGenerator[Request, None]:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=1 << 27)  # 128 MiB.
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while line := await reader.readline():
        request = cls.model_validate_json(line)
        yield request


# A Mixin is a supporting class which implements a utility on behalf of a connector.
# Mixins may implement a protocol like Python's Asynchronous Context Manager pattern:
# they're entered prior to handling any requests, and exited after all requests have
# been fully processed.
class Mixin(abc.ABC):
    async def _mixin_enter(self, log: Logger): ...
    async def _mixin_exit(self, log: Logger): ...


@dataclass
class Stopped(Exception):
    """
    Stopped is raised by a connector's handle() function to indicate that the
    connector has stopped and this process should exit.

    `error`, if set, is a fatal error condition that caused the connector to exit.
    """

    error: str | None


# BaseConnector from which all Flow Connectors inherit.
class BaseConnector(Generic[Request], abc.ABC):

    # request_class() returns the concrete class instance for Request served
    # by this connector. It's required to implement to enable parsing of
    # Requests prior to their being dispatched by the connector.
    #
    # Python classes are type-erased, so it's not possible to determine the
    # concrete class at runtime using only type annotations.
    @classmethod
    @abc.abstractclassmethod
    def request_class(cls) -> type[Request]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def handle(self, log: Logger, request: Request) -> None:
        raise NotImplementedError()

    # Serve this connector by invoking `handle()` for all incoming instances of
    # Request as a concurrent asyncio.Task using the provided `responder`.
    # When incoming `requests` are exhausted, all pending tasks are awaited
    # before returning.
    # All handler exceptions are caught and logged, and serve() returns an
    # exit code which indicates whether an error occurred.
    async def serve(
        self,
        log: Logger | None = None,
        requests: Callable[
            [type[Request]], AsyncGenerator[Request, None]
        ] = stdin_jsonl,
    ):
        if not log:
            log = init_logger()

        assert isinstance(log, Logger)  # Narrow type to non-None.

        loop = asyncio.get_running_loop()
        this_task = asyncio.current_task(loop)
        original_sigquit = signal.getsignal(signal.SIGQUIT)

        def dump_all_tasks(signum, frame):
            tasks = asyncio.all_tasks(loop)

            for task in tasks:
                if task is this_task:
                    continue

                # Reach inside the task coroutine to inject an exception, which
                # will unwind the task stack and lets us print a precise stack trace.
                try:
                    task.get_coro().throw(RuntimeError("injected SIGQUIT exception"))
                except Exception as exc:
                    msg, args = type(exc).__name__, exc.args
                    if len(args) != 0:
                        msg = f"{msg}: {args[0]}"
                    log.exception(msg, args)

                # We manually injected an exception into the coroutine,
                # so the asyncio event loop will attempt to await it again
                # and get a new "cannot reuse already awaited coroutine".
                # Cancel the task now and add a callback to clear the exception.
                task.cancel()
                task.add_done_callback(lambda task: task.exception())

            # Wake event loop.
            loop.call_soon_threadsafe(lambda: None)

        signal.signal(signal.SIGQUIT, dump_all_tasks)

        # Call _mixin_enter() on all mixed-in base classes.
        for base in self.__class__.__bases__:
            if enter := getattr(base, "_mixin_enter", None):
                await enter(self, log)

        failed = False
        try:
            async with asyncio.TaskGroup() as group:
                async for request in requests(self.request_class()):
                    group.create_task(self.handle(log, request))

        except ExceptionGroup as exc_group:
            for exc in exc_group.exceptions:
                if isinstance(exc, ValidationError):
                    if len(exc.errors) == 1:
                        log.error(exc.errors[0])
                    else:
                        log.error(
                            "Multiple validation errors:\n - "
                            + "\n - ".join(exc.errors)
                        )
                    failed = True
                elif isinstance(exc, Stopped):
                    if exc.error:
                        log.error(f"{exc.error}")
                        failed = True
                else:
                    log.error("".join(traceback.format_exception(exc)))
                    failed = True

        finally:
            # Call _mixin_exit() on all mixed-in base classes, in reverse order.
            for base in reversed(self.__class__.__bases__):
                if exit := getattr(base, "_mixin_exit", None):
                    await exit(self, log)

            # Restore the original signal handler
            signal.signal(signal.SIGQUIT, original_sigquit)

        if failed:
            raise SystemExit(1)
