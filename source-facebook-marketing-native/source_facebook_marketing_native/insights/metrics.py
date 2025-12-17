"""Metrics tracking for Facebook Insights job processing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .types import JobScope

if TYPE_CHECKING:
    from .types import InsightsJob


def _scope_counter() -> dict[JobScope, int]:
    """Factory to create a zero-initialized counter for each JobScope."""
    return {scope: 0 for scope in JobScope}


@dataclass
class JobMetrics:
    """
    Track metrics for observability across the job tree.

    Provides visibility into:
    - Overall progress (submitted, completed, split, failed)
    - Per-scope breakdown (how many jobs at each hierarchy level)
    - Tree depth (maximum split depth reached)
    - Record counts
    """

    submitted: int = 0
    completed: int = 0
    split: int = 0
    failed: int = 0
    records: int = 0
    max_depth: int = 0

    submitted_by_scope: dict[JobScope, int] = field(default_factory=_scope_counter)
    completed_by_scope: dict[JobScope, int] = field(default_factory=_scope_counter)
    pending_by_scope: dict[JobScope, int] = field(default_factory=_scope_counter)

    def track_submit(self, job: InsightsJob) -> None:
        """Track a job submission."""
        self.submitted += 1
        self.max_depth = max(self.max_depth, job.depth)
        self.submitted_by_scope[job.scope] += 1
        self.pending_by_scope[job.scope] += 1

    def track_complete(self, job: InsightsJob) -> None:
        """Track a job completion."""
        self.completed += 1
        self.completed_by_scope[job.scope] += 1
        self.pending_by_scope[job.scope] -= 1

    def track_split(self, job: InsightsJob) -> None:
        """Track a job split (remove from pending, don't count as completed)."""
        self.split += 1
        self.pending_by_scope[job.scope] -= 1

    def tree_progress_summary(self) -> str:
        """Generate tree progress summary string."""
        parts = []
        for scope in JobScope:
            submitted = self.submitted_by_scope.get(scope, 0)
            completed = self.completed_by_scope.get(scope, 0)
            if submitted > 0:
                parts.append(f"{scope.value}={completed}/{submitted}")

        if not parts:
            return "no jobs yet"

        return f"{', '.join(parts)}, max_depth={self.max_depth}"
