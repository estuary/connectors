"""Run from the connector directory: python tests/run_simulation_mutations.py.

Each mutation is applied alone to a byte snapshot, tested in a fresh process,
and restored in finally (including on timeout). No git checkout/reset is used:
pre-existing uncommitted edits are preserved. This is a manual audit helper,
not part of pytest collection. Each simulation has a five-second deadline.
"""

from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "source_amazon_selling_partner/api.py"
SIM = ROOT / "tests/test_simulation.py"


def mutate_function(source: str, function: str, old: str, new: str) -> str:
    start = source.index(f"async def {function}(")
    end = source.find("\nasync def ", start + 1)
    if end == -1:
        end = len(source)
    body = source[start:end]
    assert body.count(old) == 1, (function, old)
    return source[:start] + body.replace(old, new) + source[end:]


def main() -> None:
    # Read the actual configured default: there is no API overlap constant;
    # fetch_orders receives Advanced.overlap (currently ten minutes).
    from source_amazon_selling_partner.models import EndpointConfig
    overlap = EndpointConfig.Advanced.model_validate({}).overlap
    skip_seconds = int(overlap.total_seconds()) + 3600
    api_original, sim_original = API.read_bytes(), SIM.read_bytes()
    mutations = [
        ("M1", API, "fetch_orders", "    yield window_end\n",
         f"    yield window_end + timedelta(seconds={skip_seconds})\n"),
        ("M2", API, "backfill_financial_transactions", "            window_end = smaller_end\n",
         "            pass\n"),
        ("M3", API, "backfill_orders", '        yield f"{to_iso(window_end + TICK)}|{current_size.total_seconds()}|{window_size.total_seconds()}"\n',
         '        yield f"{to_iso(min(window_start + window_size - TICK, last_tick) + TICK)}|{current_size.total_seconds()}|{window_size.total_seconds()}"\n'),
        ("M4", API, "fetch_financial_transactions", "    yield window_end\n",
         "    yield window_end + timedelta(hours=1)\n"),
        ("M5", SIM, None, "            for name, lag in lags.items():\n",
         "            for name, lag in lags.items():\n                if name == resources.LATE:\n                    continue\n"),
        ("M6", SIM, None, "                        self.discarded += len(pending)\n",
         "                        self.committed.extend(pending)\n                        self.discarded += len(pending)\n"),
        ("M7", API, "backfill_orders", '        yield f"{to_iso(window_end + TICK)}|{current_size.total_seconds()}|{window_size.total_seconds()}"\n',
         "        yield to_iso(window_end + TICK)\n"),
        ("M8", API, "fetch_orders_sweep", "    covered = log_cursor\n",
         "    return\n    covered = log_cursor\n"),
        ("M9", API, "backfill_orders", "            current_size = window_end + TICK - window_start\n",
         "            current_size = max(current_size, window_end + TICK - window_start)\n"),
        ("M10", API, "fetch_orders_sweep", "                covered = item\n",
         "                covered = item + TICK\n"),
    ]
    survived = []
    for name, path, function, old, new in mutations:
        original = api_original if path == API else sim_original
        try:
            source = original.decode()
            if function:
                changed = mutate_function(source, function, old, new)
            else:
                assert source.count(old) == 1, name
                changed = source.replace(old, new)
            path.write_bytes(changed.encode())
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "pytest", "-q", "tests/test_simulation.py", "tests/test_orders.py", "tests/test_orders_sweep.py"],
                    cwd=ROOT, capture_output=True, text=True, timeout=5,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"{name} exceeded five seconds") from None
            # Collection/import failures are not mutation kills.
            assert "failed" in result.stdout or result.returncode == 0, result.stdout + result.stderr
            outcome = "KILLED" if result.returncode else "SURVIVED"
            log = Path(tempfile.gettempdir()) / f"amazon-simulation-{name}.log"
            log.write_text(result.stdout + result.stderr)
            print(f"{name}: {outcome}; {log}", flush=True)
            if not result.returncode:
                survived.append(name)
        finally:
            path.write_bytes(original)
            assert API.read_bytes() == api_original and SIM.read_bytes() == sim_original
    assert not survived, f"surviving mutations: {survived}"


if __name__ == "__main__":
    main()
