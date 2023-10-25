from queue import Queue
from threading import Thread
import abc
import jsonlines
import orjson
import sys
import typing as t

from .. import BaseConnector, flow
from . import request
from . import response


class Request(t.TypedDict, total=False):
    spec: request.Spec
    discover: request.Discover
    validate: request.Validate
    apply: request.Apply
    open: request.Open
    acknowledge: request.Acknowledge


class Response(t.TypedDict, total=False):
    spec: flow.Spec
    discovered: response.Discovered
    validated: response.Validated
    applied: response.Applied
    opened: response.Opened
    captured: response.Captured
    checkpoint: response.Checkpoint


class Connector(BaseConnector, metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def spec(self, spec: request.Spec) -> flow.Spec:
        raise NotImplementedError()

    @abc.abstractmethod
    def discover(self, discover: request.Discover) -> response.Discovered:
        raise NotImplementedError()

    @abc.abstractmethod
    def validate(self, validate: request.Validate) -> response.Validated:
        raise NotImplementedError()

    def apply(self, apply: request.Apply) -> response.Applied:
        return response.Applied()  # No-op.

    @abc.abstractmethod
    def open(self, open: request.Open, emit: t.Callable[[Response], None]) -> None:
        raise NotImplementedError()

    def acknowledge(self, acknowledge: request.Acknowledge) -> None:
        pass  # No-op.

    def _run(self) -> None:
        # Start a read loop in a thread which dispatches to `requests`.
        requests: Queue[Request | None] = Queue(maxsize=8)
        Thread(
            target=_read_loop, name="read loop", args=(requests,), daemon=True
        ).start()

        while True:
            if not (request := requests.get()):
                return  # EOF.
            elif (spec := request.get("spec")) is not None:
                spec_response = self.spec(spec)
                spec_response["protocol"] = 3032023
                self._emit(Response(spec=spec_response))
            elif (discover := request.get("discover")) is not None:
                self._emit(Response(discovered=self.discover(discover)))
            elif (validate := request.get("validate")) is not None:
                self._emit(Response(validated=self.validate(validate)))
            elif (apply := request.get("apply")) is not None:
                self._emit(Response(applied=self.apply(apply)))
            elif (open := request.get("open")) is not None:
                break
            else:
                raise RuntimeError("malformed request", request)

        def _emit_response(response: Response) -> None:
            self._emit(response)

            # Queue rarely has items, so optimize for the common "empty" case.
            # Otherwise non-blocking get() throws Empty, and exceptions are expensive.
            # This is racy and can false-negative but can never false-positive,
            # since we're the only queue consumer.
            if requests.qsize() == 0:
                return

            if not (next := requests.get()):
                return  # EOF
            elif (acknowledge := next.get("acknowledge")) is not None:
                self.acknowledge(acknowledge)
            else:
                raise RuntimeError("malformed request", request)

        self.open(open, _emit_response)


def _read_loop(queue: Queue[Request | None]):
    request: Request
    for request in jsonlines.Reader(sys.stdin, loads=orjson.loads):
        queue.put(request)

    queue.put(None)  # Signal EOF.
