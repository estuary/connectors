"""Turn `pyright --outputjson` into GitHub Actions annotations.

pyright has no GitHub output format of its own, so without this its findings
only ever reach the raw job log. Everything is emitted at `::warning`,
whatever severity pyright assigned: the connectors carry a large pre-existing
backlog, so these are reported rather than gated, and a step that cannot fail
should not be printing errors.

A per-rule tally goes to the job summary, which survives the annotation cap
that hides all but the first handful in the run UI.

Usage: pyright --outputjson <paths> | python3 pyright_annotations.py <label>
"""

import collections
import json
import os
import sys


def main() -> int:
    label = sys.argv[1] if len(sys.argv) > 1 else "pyright"
    report = json.load(sys.stdin)
    diagnostics = report.get("generalDiagnostics", [])
    root = os.getcwd()

    counts: collections.Counter[str] = collections.Counter()
    for d in diagnostics:
        counts[d.get("rule", "(no rule)")] += 1

        path = os.path.relpath(d["file"], root)
        # pyright's ranges are 0-based; GitHub's are 1-based.
        start = d.get("range", {}).get("start", {})
        line = start.get("line", 0) + 1
        col = start.get("character", 0) + 1
        # A newline would terminate the workflow command early.
        message = d["message"].replace("\n", " ")
        rule = d.get("rule", "")
        # GitHub renders a workflow command as `##[warning]<message>` and keeps
        # file/line only as annotation metadata, so the location is repeated
        # inside the message -- the same shape ruff's `--output-format=github`
        # produces, and the reason its output is readable in a raw job log.
        located = f"{path}:{line}:{col}: {rule} {message}".strip()
        print(f"::warning file={path},line={line},col={col}::{located}")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            _ = f.write(f"### pyright: {label} -- {len(diagnostics)} findings\n\n")
            if counts:
                _ = f.write("| count | rule |\n| ---: | --- |\n")
                for rule, n in counts.most_common():
                    _ = f.write(f"| {n} | {rule} |\n")
                _ = f.write("\n")

    # Always succeed: this step reports, it does not gate.
    return 0


if __name__ == "__main__":
    sys.exit(main())
