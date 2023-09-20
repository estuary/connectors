import abc
import orjson
import os
import sys
import typing as t
from .logger import init_logger

logger = init_logger()


class BaseConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _run(self) -> None:
        raise NotImplemented

    def _emit(self, response: t.Any) -> None:
        # TODO(johnny): Make this buffered?
        os.write(
            1,
            orjson.dumps(
                response,
                # Map non-string dict keys into str
                option=orjson.OPT_NON_STR_KEYS
                # Prefer 'Z' over '+00:00'.
                | orjson.OPT_UTC_Z
                # Add a newline for newline JSON.
                | orjson.OPT_APPEND_NEWLINE,
                # Map unhandled JSON types through __str__() if available, or repr().
                default=str,
            ),
        )

    def main(self) -> None:
        try:
            self._run()

        except ValidateError as exc:
            logger.exception(exc.message)
            sys.exit(-1)

        except Exception as exc:
            msg, args = type(exc).__name__, exc.args
            if len(args) != 0:
                msg = f"{msg}: {args[0]}"

            logger.exception(msg, args)
            sys.exit(-1)


class ValidateError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
