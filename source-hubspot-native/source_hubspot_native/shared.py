from datetime import datetime, UTC


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def chunk_props(props: list[str], max_bytes: int) -> list[list[str]]:
    result: list[list[str]] = []

    current_chunk: list[str] = []
    current_size = 0

    for p in props:
        sz = len(p.encode("utf-8"))

        if current_size + sz > max_bytes:
            result.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(p)
        current_size += sz

    if current_chunk:
        result.append(current_chunk)

    return result