"""Unit tests for the benchmark results builder.

Run with: python3 -m unittest tests/benchmark/materialize/test_results.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import results  # noqa: E402


def preview_log(transaction_seconds):
    """Render a preview log covering len(transaction_seconds) rounds.

    The shape mirrors a real run, which matters because every quirk below is
    one the parser has to survive:

    - Round 0 is the fixture's leading bare {"commit": true}. It reads no store
      requests and commits in milliseconds.
    - ACK intents for round N are written at the start of round N+1, so the
      final round has no ACK line at all.
    - The run ends the moment the last round's commit is durable: the actor
      goes Persisted -> Stop and exits without a further round.
    """
    t = 0.0
    out = []

    def emit(seconds, line):
        nonlocal t
        t += seconds
        stamp = f"2026-08-25T18:{int(t // 60):02d}:{t % 60:06.3f}Z"
        out.append(f"{stamp}  INFO {line}")

    # The process logs from startup; apply_seconds is measured from its first
    # line, well before the recovery commit.
    emit(0.0, "flowctl::raw::preview_next: starting preview")
    emit(9.4, "ops: connector applied")
    emit(1.1, "ops: started materialization recovery commit fields={\"round\":0}")
    emit(0.0, "runtime_next::leader::materialize::actor: completed ACK intents write track=\"leader\"")
    # Round 0: the empty leading cycle.
    emit(0.004, 'flowctl::raw::preview_next::publish: transaction stats stats={"materialize":{}}')
    emit(0.002, 'ops: started materialization commit fields={"round":0}')
    emit(0.001, "ops: finished materialization commit")
    emit(0.0, "runtime_next::leader::materialize::actor: completed ACK intents write track=\"leader\"")

    for i, secs in enumerate(transaction_seconds, start=1):
        emit(0.15, f'ops: started reading store requests fields={{"round":{i}}}')
        emit(secs * 0.97, f'ops: finished reading store requests fields={{"round":{i}}}')
        emit(secs * 0.03, 'flowctl::raw::preview_next::publish: transaction stats stats={"materialize":{}}')
        emit(0.001, f'ops: started materialization commit fields={{"round":{i}}}')
        if i < len(transaction_seconds):
            emit(0.002, "ops: finished materialization commit")
            emit(0.0, "runtime_next::leader::materialize::actor: completed ACK intents write track=\"leader\"")
        else:
            # The last round commits and the actor stops; no ACK follows.
            emit(0.002, "ops: finished materialization commit")
            emit(0.001, "runtime_next::leader::materialize::actor: materialize Actor::serve exiting; broadcasting Stopped")
    return "\n".join(out) + "\n"


class TestBoundariesFromLog(unittest.TestCase):
    def bounds_for(self, transaction_seconds):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as f:
            f.write(preview_log(transaction_seconds))
            path = f.name
        try:
            return results.boundaries_from_log(path)
        finally:
            os.unlink(path)

    def test_every_generated_transaction_is_measured(self):
        """A run of N transactions must yield N bounds.

        The last round's ACK intents are never written, so pairing ACK lines
        loses that transaction; and round 0 is an empty cycle, so pairing from
        the first ACK also credits a whole transaction's bytes to a few
        milliseconds.
        """
        walls = [300.0, 280.0, 290.0]
        _, bounds, observed = self.bounds_for(walls)
        self.assertEqual(len(bounds), len(walls))
        self.assertEqual(observed, len(walls))

    def test_no_bound_collapses_to_the_empty_cycle(self):
        """None of the bounds may be the millisecond-long round 0."""
        walls = [300.0, 280.0, 290.0]
        _, bounds, _ = self.bounds_for(walls)
        measured = [(end - start) / 1e9 for start, end in bounds]
        for i, secs in enumerate(measured):
            self.assertGreater(secs, 1.0, f"bound {i} is {secs}s: the empty cycle")

    def test_walls_track_the_transactions_that_produced_them(self):
        walls = [300.0, 280.0, 290.0]
        _, bounds, _ = self.bounds_for(walls)
        measured = [(end - start) / 1e9 for start, end in bounds]
        for want, got in zip(walls, measured):
            self.assertAlmostEqual(want, got, delta=1.0)

    def test_apply_seconds_covers_startup_and_the_empty_cycle(self):
        apply_s, _, _ = self.bounds_for([300.0, 280.0])
        self.assertGreater(apply_s, 10.0)
        self.assertLess(apply_s, 12.0)

    def test_a_log_with_no_transactions_yields_nothing(self):
        apply_s, bounds, observed = self.bounds_for([])
        self.assertEqual(bounds, [])
        self.assertEqual(observed, 0)


if __name__ == "__main__":
    unittest.main()
