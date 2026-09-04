import argparse
import json
import os
import signal
import socket
import sys

# The SDK's native core reads these at first import: logs must go to stderr
# (stdout carries the ready handshake) at a level following the connector's.
os.environ.setdefault("SS_LOG_TARGET", "stderr")
os.environ.setdefault("SS_LOG_LEVEL", "warn")

from .ops_logging import setup_logging


def main() -> None:
    logger = setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--uds", metavar="PATH", required=True, help="listen on this unix domain socket")
    args = parser.parse_args()

    # SIGTERM is the supervisor asking us to go; buffered SDK data is
    # recoverable via offset tokens so there is nothing to save.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # The auth token arrives on stdin so it appears in no argv or environ.
    auth_token = sys.stdin.readline().strip()

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(args.uds)
    server.listen(1)
    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    conn, _ = server.accept()
    server.close()
    logger.info("sidecar ready")

    from .server import Server

    Server(conn, auth_token).serve()


if __name__ == "__main__":
    main()
