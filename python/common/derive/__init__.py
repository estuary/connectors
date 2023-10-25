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
    validate: request.Validate
    open: request.Open
    read: request.Read
    flush: request.Flush
    startCommit: request.StartCommit


class Response(t.TypedDict, total=False):
    spec: flow.Spec
    validated: response.Validated
    opened: response.Opened
    published: response.Published
    flushed: response.Flushed
    startedCommit: response.StartedCommit


class Connector(BaseConnector, metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def spec(self, spec: request.Spec) -> flow.Spec:
        raise NotImplemented

    @abc.abstractmethod
    def validate(self, validate: request.Validate) -> response.Validated:
        raise NotImplemented

    @abc.abstractmethod
    def open(self, open: request.Open) -> None:
        raise NotImplemented

    @abc.abstractmethod
    def read(
        self, read: request.Read, emit: t.Callable[[response.Published], None]
    ) -> None:
        raise NotImplemented

    def flush(
        self, flush: request.Flush, emit: t.Callable[[response.Published], None]
    ) -> response.Flushed:
        return response.Flushed()

    def startCommit(self, start_commit: request.StartCommit) -> response.StartedCommit:
        return response.StartedCommit(
            state=flow.ConnectorState(updated={}, mergePatch=False)
        )

    def _run(self) -> None:
        requests = jsonlines.Reader(sys.stdin, loads=orjson.loads)
        request: Request

        for request in requests:
            if (spec := request.get("spec")) is not None:
                spec_response = self.spec(spec)
                spec_response["protocol"] = 3032023
                self._emit(Response(spec=spec_response))
            elif (validate := request.get("validate")) is not None:
                self._emit(Response(validated=self.validate(validate)))
            elif (open := request.get("open")) is not None:
                self.open(open)
                self._emit(Response(opened=response.Opened()))
                break

        def _emit_published(published: response.Published) -> None:
            self._emit(Response(published=published))

        for request in requests:
            if (read := request.get("read")) is not None:
                self.read(read, _emit_published)
            elif (flush := request.get("flush")) is not None:
                self._emit(Response(flushed=self.flush(flush, _emit_published)))
            elif (start := request.get("startCommit")) is not None:
                self._emit(Response(startedCommit=self.startCommit(start)))
            else:
                raise RuntimeError("malformed request", request)
