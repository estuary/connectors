"""Metrics tracking for Facebook Insights job processing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .types import JobScope

if TYPE_CHECKING:
    from .types import InsightsJob


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

    # Per-scope tracking for tree progress visibility
    submitted_by_scope: dict[JobScope, int] | None = None
    completed_by_scope: dict[JobScope, int] | None = None
    pending_by_scope: dict[JobScope, int] | None = None

    def __post_init__(self) -> None:
        """Initialize per-scope tracking dicts."""
        self.submitted_by_scope = {scope: 0 for scope in JobScope}
        self.completed_by_scope = {scope: 0 for scope in JobScope}
        self.pending_by_scope = {scope: 0 for scope in JobScope}

    def track_submit(self, job: InsightsJob) -> None:
        """Track a job submission."""
        self.submitted += 1
        self.max_depth = max(self.max_depth, job.depth)
        if self.submitted_by_scope is not None:
            self.submitted_by_scope[job.scope] += 1
        if self.pending_by_scope is not None:
            self.pending_by_scope[job.scope] += 1

    def track_complete(self, job: InsightsJob) -> None:
        """Track a job completion."""
        self.completed += 1
        if self.completed_by_scope is not None:
            self.completed_by_scope[job.scope] += 1
        if self.pending_by_scope is not None:
            self.pending_by_scope[job.scope] -= 1

    def track_split(self, job: InsightsJob) -> None:
        """Track a job split (remove from pending, don't count as completed)."""
        self.split += 1
        if self.pending_by_scope is not None:
            self.pending_by_scope[job.scope] -= 1

    def tree_progress_summary(self) -> str:
        """Generate tree progress summary string."""
        if not self.submitted_by_scope or not self.completed_by_scope:
            return "no progress data"

        parts = []
        for scope in JobScope:
            submitted = self.submitted_by_scope.get(scope, 0)
            completed = self.completed_by_scope.get(scope, 0)
            if submitted > 0:
                parts.append(f"{scope.value}={completed}/{submitted}")

        if not parts:
            return "no jobs yet"

        return f"{', '.join(parts)}, max_depth={self.max_depth}"
