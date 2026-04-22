#!/usr/bin/env python3
"""
Materialization benchmark fixture generator.

Reads a scenario YAML and emits newline-delimited JSON suitable for
`flowctl preview --fixture`. Each transaction in the scenario is
emitted as a sequence of [collection, document] lines terminated by
{"commit": true}.

Documents are padded to hit a precise byte size so that "10 GB" of
configured load really moves 10 GB through the pipeline. Overlaps
between transactions allow a transaction to update or delete keys
written by a prior transaction; multiple overlap entries per
transaction are supported.

Usage:
    generate.py --scenario scenarios/large-3tx.yaml [--seed N]
                [--output path | -]    # default: stdout
                [--state path]         # optional: dump per-tx state JSON
"""

import argparse
import json
import random
import string
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, TextIO

import yaml


# ---------------------------------------------------------------------------
# Sizing helpers
# ---------------------------------------------------------------------------

_SUFFIXES = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}


def parse_size(value: int | str) -> int:
    """Parse a size value: int (bytes) or string with B/KB/MB/GB/TB suffix."""
    if isinstance(value, int):
        return value
    s = value.strip().replace("_", "")
    for suffix in ("TB", "GB", "MB", "KB", "B"):
        if s.upper().endswith(suffix):
            n = s[: -len(suffix)].strip()
            return int(float(n) * _SUFFIXES[suffix])
    return int(s)


def parse_count(value: Any) -> int:
    """Parse a count: int or string (allowing _ separators like 1_000_000)."""
    if isinstance(value, int):
        return value
    return int(str(value).replace("_", ""))


# ---------------------------------------------------------------------------
# Scenario model
# ---------------------------------------------------------------------------


_VALID_KEY_TYPES = ("integer", "uuid")


@dataclass
class CollectionSpec:
    name: str
    schema: dict
    key: list[str]
    key_field: str = "id"
    payload_field: str = "payload"
    key_type: str = "integer"  # "integer" or "uuid"


@dataclass
class OverlapSpec:
    with_tx: int
    fraction: float
    op: Literal["u", "d"]


@dataclass
class TxSpec:
    index: int
    collection: str
    doc_count: int
    doc_size: int
    overlaps: list[OverlapSpec] = field(default_factory=list)
    op: str = "c"  # op for the "fresh" remainder


def _resolve_sizing(raw: dict, idx: int) -> tuple[int, int]:
    """Resolve (doc_count, doc_size) from any two of doc_count/doc_size/total_size."""
    have_count = "doc_count" in raw
    have_size = "doc_size" in raw
    have_total = "total_size" in raw

    given = sum([have_count, have_size, have_total])
    if given < 2:
        raise ValueError(
            f"transaction[{idx}]: must specify at least two of doc_count, doc_size, total_size"
        )

    if have_count and have_size:
        doc_count = parse_count(raw["doc_count"])
        doc_size = parse_size(raw["doc_size"])
    elif have_count and have_total:
        doc_count = parse_count(raw["doc_count"])
        total = parse_size(raw["total_size"])
        doc_size = total // doc_count
    else:  # have_size and have_total
        doc_size = parse_size(raw["doc_size"])
        total = parse_size(raw["total_size"])
        doc_count = total // doc_size

    if doc_count <= 0:
        raise ValueError(f"transaction[{idx}]: doc_count must be > 0 (got {doc_count})")
    if doc_size <= 0:
        raise ValueError(f"transaction[{idx}]: doc_size must be > 0 (got {doc_size})")
    return doc_count, doc_size


def load_scenario(path: str) -> tuple[dict[str, CollectionSpec], list[TxSpec]]:
    with open(path) as f:
        raw = yaml.safe_load(f)

    collections: dict[str, CollectionSpec] = {}
    for c in raw.get("collections", []):
        key_type = c.get("key_type", "integer")
        if key_type not in _VALID_KEY_TYPES:
            raise ValueError(
                f"collection {c['name']}: key_type must be one of {_VALID_KEY_TYPES} (got {key_type!r})"
            )
        spec = CollectionSpec(
            name=c["name"],
            schema=c["schema"],
            key=c["key"],
            key_field=c.get("key_field", "id"),
            payload_field=c.get("payload_field", "payload"),
            key_type=key_type,
        )
        if spec.key_field not in spec.schema.get("properties", {}):
            raise ValueError(
                f"collection {spec.name}: key_field '{spec.key_field}' not in schema.properties"
            )
        if spec.payload_field not in spec.schema.get("properties", {}):
            raise ValueError(
                f"collection {spec.name}: payload_field '{spec.payload_field}' not in schema.properties"
            )
        collections[spec.name] = spec

    if not collections:
        raise ValueError("scenario must define at least one collection")

    default_collection = next(iter(collections))

    transactions: list[TxSpec] = []
    for i, t in enumerate(raw.get("transactions", [])):
        doc_count, doc_size = _resolve_sizing(t, i)
        overlaps = [
            OverlapSpec(with_tx=int(o["with"]), fraction=float(o["fraction"]), op=o["op"])
            for o in t.get("overlaps", [])
        ]
        for o in overlaps:
            if o.op not in ("u", "d"):
                raise ValueError(f"transaction[{i}]: overlap op must be 'u' or 'd' (got {o.op!r})")
            if o.with_tx >= i:
                raise ValueError(
                    f"transaction[{i}]: overlap.with={o.with_tx} must reference an earlier tx"
                )
            if not (0 < o.fraction <= 1):
                raise ValueError(
                    f"transaction[{i}]: overlap.fraction must be in (0, 1] (got {o.fraction})"
                )
        total_overlap = sum(o.fraction for o in overlaps)
        if total_overlap > 1.0 + 1e-9:
            raise ValueError(
                f"transaction[{i}]: overlap fractions sum to {total_overlap:.4f} > 1"
            )

        col = t.get("collection", default_collection)
        if col not in collections:
            raise ValueError(f"transaction[{i}]: unknown collection {col!r}")

        transactions.append(
            TxSpec(
                index=i,
                collection=col,
                doc_count=doc_count,
                doc_size=doc_size,
                overlaps=overlaps,
                op=t.get("op", "c"),
            )
        )

    if not transactions:
        raise ValueError("scenario must define at least one transaction")
    return collections, transactions


# ---------------------------------------------------------------------------
# Document generation
#
# Hot-path design: we never call json.dumps or an RNG per-doc. Instead we
# precompute static line fragments ("prefix", "middle", "suffix") around
# the two variable spots — the integer key and the payload bytes — and
# at emit time write (prefix + str(key) + middle + pool_slice + suffix).
#
# The payload is sliced from a single preallocated entropy pool that is
# randomised once at startup from the run's seed. Per-doc work is now
# O(doc_size) in memcpy (for the pool slice) plus O(log key) for the key
# string — no per-byte Python-bytecode loops.
# ---------------------------------------------------------------------------

# Alphabet that encodes 1 char -> 1 byte in JSON (no escaping).
_PAYLOAD_ALPHABET = string.ascii_letters + string.digits
# Knuth-style multiplier used to scatter per-doc pool offsets.
_POOL_MIX = 2654435761
# Fixed namespace for deterministic UUID generation from integer keys.
_UUID_NAMESPACE = uuid.UUID("a3f2b8c1-7d4e-4f9a-b6c8-1e2d3f4a5b6c")


def _int_key_to_uuid(key: int) -> str:
    """Deterministic UUID from an integer key. Uses UUID v5 (SHA-1 based)
    so the same key always produces the same UUID across runs."""
    return str(uuid.uuid5(_UUID_NAMESPACE, key.to_bytes(8, "little", signed=True)))


def _fixed_scalar(prop: str, schema: dict) -> Any:
    """Deterministic schema-appropriate value for a non-key, non-payload,
    non-_meta scalar. Called once per scenario at template-build time, so
    the value is baked into the static line fragments. No RNG at emit time."""
    t = schema.get("type", "string")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), "string")

    if t == "integer":
        # Arbitrary but stable value; width matters because it's baked in.
        return 424242
    if t == "number":
        return 3.14
    if t == "boolean":
        return True
    # default: string
    return f"{prop}_value"


def _make_pool(seed: int, size: int) -> str:
    """Precompute a deterministic entropy pool of `size` ASCII bytes.
    Uses bulk random.randbytes (C path) plus bytes.translate (C path) so
    construction is linear in size with no Python-level per-byte loop."""
    rng = random.Random(seed)
    alphabet = _PAYLOAD_ALPHABET.encode("ascii")
    n_alpha = len(alphabet)
    raw = rng.randbytes(size)
    # Single-pass byte-to-alphabet mapping via translate() (runs in C).
    # Slight bias from 256 mod 62 but harmless for a benchmark fixture.
    table = bytes(alphabet[b % n_alpha] for b in range(256))
    return raw.translate(table).decode("ascii")


def _build_emit_template(
    col: CollectionSpec, op: str
) -> tuple[str, str, str, int]:
    """Return (prefix, middle, suffix, fixed_overhead) for one (collection, op).

    Emitted line: prefix + str(key) + middle + payload_slice + suffix
    (suffix already includes the trailing newline).

    fixed_overhead = len(prefix) + len(middle) + len(suffix), so
    payload_len = doc_size - fixed_overhead - len(str(key))."""
    props = col.schema.get("properties", {})
    if col.key_field not in props:
        raise ValueError(f"collection {col.name}: key_field {col.key_field!r} not in schema.properties")
    if col.payload_field not in props:
        raise ValueError(f"collection {col.name}: payload_field {col.payload_field!r} not in schema.properties")

    # Ensure key comes before payload in property order (our templating
    # assumes prefix-then-middle split order).
    ordered = [name for name in props if name != "_meta"]
    if ordered.index(col.key_field) > ordered.index(col.payload_field):
        raise ValueError(
            f"collection {col.name}: key_field must appear before payload_field in schema.properties"
        )

    # Build the DOC fragments (the "{...}" object) first so overhead can
    # be measured against the doc only. Envelope is added outside.
    doc_parts: list[str] = ["{"]
    key_slot: int = -1
    payload_slot: int = -1
    first = True

    key_is_string = col.key_type == "uuid"

    for name in ordered:
        sep = "" if first else ","
        first = False
        if name == col.key_field:
            if key_is_string:
                doc_parts.append(f'{sep}"{name}":"')
                key_slot = len(doc_parts)
                doc_parts.append("")  # filled with key string at emit time
                doc_parts.append('"')
            else:
                doc_parts.append(f'{sep}"{name}":')
                key_slot = len(doc_parts)
                doc_parts.append("")  # filled with str(key) at emit time
        elif name == col.payload_field:
            doc_parts.append(f'{sep}"{name}":"')
            payload_slot = len(doc_parts)
            doc_parts.append("")  # filled with pool_slice at emit time
            doc_parts.append('"')
        else:
            val = _fixed_scalar(name, props[name])
            doc_parts.append(f'{sep}"{name}":{json.dumps(val, separators=(",", ":"))}')

    # Always emit _meta.op last — the fixture protocol expects it, and
    # the schema's if/then delete annotation keys on /_meta/op == "d".
    meta_sep = "" if first else ","
    doc_parts.append(f'{meta_sep}"_meta":{{"op":"{op}"}}')
    doc_parts.append("}")

    doc_prefix = "".join(doc_parts[:key_slot])
    doc_middle = "".join(doc_parts[key_slot + 1 : payload_slot])
    doc_suffix = "".join(doc_parts[payload_slot + 1 :])
    # `overhead` measures the doc's static bytes only -- so that
    # payload_len = doc_size - overhead - len(str(key)) makes each
    # emitted DOC exactly doc_size bytes (the fixture envelope is extra).
    overhead = len(doc_prefix) + len(doc_middle) + len(doc_suffix)

    # Wrap the doc in the fixture envelope: ["<collection>",<doc>]\n.
    envelope_prefix = f'["{col.name}",'
    envelope_suffix = "]\n"
    prefix = envelope_prefix + doc_prefix
    middle = doc_middle
    suffix = doc_suffix + envelope_suffix
    return prefix, middle, suffix, overhead


# ---------------------------------------------------------------------------
# Key allocation
# ---------------------------------------------------------------------------


@dataclass
class CollectionState:
    next_key: int = 0
    # Per-tx key ranges allocated by fresh creates. (start, count).
    # Using ranges keeps memory O(num_tx) regardless of doc count.
    tx_ranges: list[tuple[int, int]] = field(default_factory=list)

    def allocate_fresh(self, n: int) -> tuple[int, int]:
        start = self.next_key
        self.next_key += n
        return start, n

    def record_tx(self, tx_idx: int, start: int, count: int) -> None:
        # Ensure list is long enough.
        while len(self.tx_ranges) <= tx_idx:
            self.tx_ranges.append((0, 0))
        self.tx_ranges[tx_idx] = (start, count)

    def sample_from_tx(
        self, tx_idx: int, n: int, exclude: set[int], rng: random.Random
    ) -> list[int]:
        """Sample n keys from tx_idx's key range, excluding `exclude`."""
        start, count = self.tx_ranges[tx_idx]
        if count == 0:
            raise ValueError(f"cannot overlap with tx {tx_idx}: it allocated no keys")
        # Fast path: when exclude is empty we can sample directly from range().
        if not exclude:
            if n > count:
                raise ValueError(
                    f"overlap requests {n} keys from tx {tx_idx} which only has {count}"
                )
            return rng.sample(range(start, start + count), n)
        # With exclusions, fall back to building the candidate list. This
        # is acceptable because cross-overlap exclusions within one tx are
        # bounded by the tx's overlap size (not by total doc count).
        candidates = [k for k in range(start, start + count) if k not in exclude]
        if n > len(candidates):
            raise ValueError(
                f"overlap requests {n} keys from tx {tx_idx} after exclusions, "
                f"only {len(candidates)} available"
            )
        return rng.sample(candidates, n)


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------


@dataclass
class OverlapPlan:
    op: str
    src_tx: int
    keys: list[int]


def _resolve_op_plan(
    tx: TxSpec, state: CollectionState, rng: random.Random
) -> tuple[list[OverlapPlan], int, int]:
    """
    Returns (overlap_plans, fresh_start, fresh_count).
    Each OverlapPlan groups the keys drawn for one overlap entry.
    Fresh creates follow with sequential keys [fresh_start, fresh_start + fresh_count).
    """
    used_per_tx: dict[int, set[int]] = {}

    # Compute per-overlap counts up front so the fresh remainder is exact.
    counts = [int(round(tx.doc_count * o.fraction)) for o in tx.overlaps]
    total_overlap = sum(counts)
    if total_overlap > tx.doc_count:
        # Trim the last overlap to fit (rounding error).
        counts[-1] -= total_overlap - tx.doc_count
        total_overlap = tx.doc_count
    fresh_count = tx.doc_count - total_overlap

    plans: list[OverlapPlan] = []
    for o, n in zip(tx.overlaps, counts):
        if n <= 0:
            plans.append(OverlapPlan(op=o.op, src_tx=o.with_tx, keys=[]))
            continue
        used = used_per_tx.setdefault(o.with_tx, set())
        keys = state.sample_from_tx(o.with_tx, n, used, rng)
        used.update(keys)
        plans.append(OverlapPlan(op=o.op, src_tx=o.with_tx, keys=keys))

    if fresh_count > 0:
        fresh_start, _ = state.allocate_fresh(fresh_count)
    else:
        fresh_start = state.next_key
    state.record_tx(tx.index, fresh_start, fresh_count)
    return plans, fresh_start, fresh_count


def emit(
    collections: dict[str, CollectionSpec],
    transactions: list[TxSpec],
    out: TextIO,
    seed: int = 0,
) -> dict:
    """Write the fixture to `out`. Returns a state dict."""
    states: dict[str, CollectionState] = {name: CollectionState() for name in collections}
    state_log: dict = {"transactions": []}

    # Entropy pool: >= 2 * largest doc_size in the scenario, >= 4 MB floor.
    # Larger pool => more distinct per-doc payload slices. A fixed pool
    # keeps peak memory bounded regardless of total bytes emitted.
    max_doc_size = max(tx.doc_size for tx in transactions)
    pool_size = max(2 * max_doc_size, 4 * 1024 * 1024)
    pool = _make_pool(seed, pool_size)

    # Precompute per-(collection, op) line templates. Computed once up front,
    # then the emit loop just does string concatenation + writes.
    templates: dict[tuple[str, str], tuple[str, str, str, int]] = {}
    for tx in transactions:
        col = collections[tx.collection]
        for op in {tx.op} | {o.op for o in tx.overlaps}:
            if (col.name, op) not in templates:
                templates[(col.name, op)] = _build_emit_template(col, op)

    # Initial empty commit so the connector applies the spec before data flows.
    out.write('{"commit":true}\n')

    for tx in transactions:
        col = collections[tx.collection]
        cstate = states[tx.collection]
        # Per-tx RNG is used only to sample overlap key sets. The emit
        # path does no per-doc RNG — payload entropy comes from pool slice.
        tx_rng = random.Random(seed * 1_000_003 + tx.index)

        plans, fresh_start, fresh_count = _resolve_op_plan(tx, cstate, tx_rng)

        state_log["transactions"].append(
            {
                "index": tx.index,
                "collection": tx.collection,
                "doc_count": tx.doc_count,
                "doc_size": tx.doc_size,
                "fresh": {"start": fresh_start, "count": fresh_count, "op": tx.op},
                "overlaps": [
                    {"op": p.op, "with": p.src_tx, "count": len(p.keys)}
                    for p in plans
                ],
            }
        )

        write = out.write
        doc_size = tx.doc_size
        format_key = _int_key_to_uuid if col.key_type == "uuid" else str

        def _emit_run(keys, op: str) -> None:
            prefix, middle, suffix, overhead = templates[(col.name, op)]
            for key in keys:
                key_s = format_key(key)
                pad = doc_size - overhead - len(key_s)
                if pad < 0:
                    raise ValueError(
                        f"transaction[{tx.index}]: doc_size={doc_size} smaller than minimum "
                        f"({overhead + len(key_s)}) for key={key} op={op!r}; "
                        f"increase doc_size or shrink schema/key"
                    )
                off = (key * _POOL_MIX) % (pool_size - pad + 1) if pad > 0 else 0
                write(prefix)
                write(key_s)
                write(middle)
                if pad > 0:
                    write(pool[off : off + pad])
                write(suffix)

        # Emit overlap docs (grouped by overlap entry).
        for plan in plans:
            _emit_run(plan.keys, plan.op)

        # Emit fresh creates.
        _emit_run(range(fresh_start, fresh_start + fresh_count), tx.op)

        out.write('{"commit":true}\n')
        out.flush()

    return state_log


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _maximize_pipe_buffer(f) -> None:
    """On Linux, enlarge the kernel pipe buffer to reduce context-switch
    overhead between the generator and flowctl. Silently no-ops on macOS
    and other platforms that don't support F_SETPIPE_SZ."""
    try:
        import fcntl

        F_SETPIPE_SZ = 1031  # Linux-specific
        # Read the system ceiling (default 1 MB on most distros).
        try:
            with open("/proc/sys/fs/pipe-max-size") as pm:
                max_size = int(pm.read().strip())
        except (FileNotFoundError, ValueError):
            max_size = 1024 * 1024  # 1 MB fallback
        fcntl.fcntl(f.fileno(), F_SETPIPE_SZ, max_size)
    except (ImportError, OSError):
        pass  # not Linux, or fd isn't a pipe — nothing to do


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scenario", required=True, help="Path to scenario YAML")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--output", default="-", help="Output path (default: stdout)")
    p.add_argument("--state", default=None, help="Optional path to write state JSON")
    args = p.parse_args(argv)

    collections, transactions = load_scenario(args.scenario)

    if args.output == "-":
        state = emit(collections, transactions, sys.stdout, seed=args.seed)
    else:
        with open(args.output, "w", buffering=128 * 1024) as f:
            _maximize_pipe_buffer(f)
            state = emit(collections, transactions, f, seed=args.seed)

    if args.state:
        with open(args.state, "w") as f:
            json.dump(state, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
