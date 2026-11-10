#!/usr/bin/env python3
"""
Compile Go connectors outside Docker for go.yaml, from what their Dockerfiles do.

Each Go connector's Dockerfile compiles it in a `builder` stage and copies the
binary into a runtime stage. CI compiles every connector once, on the runner,
and the test jobs build each image from its Dockerfile with the `builder` stage
replaced by a directory holding what the runtime stage copies from it
(`docker buildx build --build-context builder=<dir>`). The Dockerfiles are
unchanged, so local builds still compile in Docker.

    go_build.py mode <connector>
        Prints `prebuilt` or `docker`. A connector whose builder stage needs
        more than Go (apt packages, `go run` installers) keeps its Docker build.

    go_build.py build --out <dir> <connector>...
        Compiles each prebuilt connector into <dir>/<connector>/:
          ctx/      the `builder` stage stand-in: the binaries the Dockerfile
                    builds, at the paths its runtime stage copies them from
          tests/    a test binary per package of the connector with tests
          tools/    programs its tests/<connector>/*.sh scripts `go run`
          go.txt    for `prebuilt-go/go`: which directory each binary and test
                    binary stands in for
          unit-tests.sh  the Dockerfile's `go test` runs of the connector's own
                    packages, which the test job runs with `prebuilt-go/go`
        and vets the connectors' packages as `go test` would.

    go_build.py helper-tests <connector>...
        Prints the `go test` commands for the in-repo packages the connectors
        depend on, each once: what their Dockerfiles test outside their own
        directory, the runner-side helper package tests, and, for
        materializations, the shared packages tests/materialize/run.sh tests.
"""

import argparse
import concurrent.futures
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

MODULE = "github.com/estuary/connectors"

# The analyzers `go test` runs before testing a package.
VET_ANALYZERS = [
    "-atomic", "-bool", "-buildtags", "-directive", "-errorsas",
    "-ifaceassert", "-nilfunc", "-printf", "-stringintconv", "-tests",
]

# What tests/materialize/run.sh tests besides the connector.
MATERIALIZE_SHARED = ["./go/...", "./filesink/...", "./materialize-boilerplate/..."]

# The Debian the golang images are built on, for files a runtime stage copies
# from the builder's system rather than from /builder.
DEBIAN_IMAGE = "debian:trixie-slim"


@dataclass
class GoTest:
    flags: list[str]
    patterns: list[str]
    env: dict[str, str]


@dataclass
class Plan:
    connector: str
    docker_only: bool = False
    tags: str = ""
    # Output name under /builder -> package pattern.
    builds: dict[str, str] = field(default_factory=dict)
    tests: list[GoTest] = field(default_factory=list)
    # Paths the runtime stage copies from the builder stage.
    copies: list[str] = field(default_factory=list)


def logical_lines(text: str) -> list[str]:
    lines, cur = [], ""
    for raw in text.splitlines():
        line = raw.strip()
        if not cur and (not line or line.startswith("#")):
            continue
        if line.endswith("\\"):
            cur += line[:-1] + " "
            continue
        lines.append(cur + line)
        cur = ""
    if cur:
        lines.append(cur)
    return lines


def expand(value: str, args: dict[str, str]) -> str:
    return re.sub(r"\$\{?(\w+)\}?", lambda m: args.get(m.group(1), ""), value)


def plan(connector: str) -> Plan:
    p = Plan(connector)
    stage = None
    args: dict[str, str] = {}
    env: dict[str, str] = {}
    for line in logical_lines(Path(connector, "Dockerfile").read_text()):
        word, _, rest = line.partition(" ")
        word = word.upper()
        if word == "FROM":
            parts = rest.split()
            stage = parts[-1] if len(parts) >= 3 and parts[-2].upper() == "AS" else None
            continue
        if word == "COPY" and stage != "builder":
            m = re.match(r"--from=builder\s+(.*)", rest)
            if m:
                p.copies.extend(m.group(1).split()[:-1])
            continue
        if stage != "builder":
            continue
        if word == "ARG":
            k, _, v = rest.partition("=")
            args[k.strip()] = v.strip()
        elif word == "ENV":
            k, _, v = rest.partition("=")
            if k.strip() != "PATH":
                v = expand(v.strip().strip('"'), args | env)
                env[k.strip()] = v
                args[k.strip()] = v
        elif word == "RUN":
            argv = shlex.split(rest)
            while argv and argv[0].startswith("--"):  # --mount=...
                argv.pop(0)
            if argv[:2] == ["go", "build"]:
                out = pkg = None
                i = 2
                while i < len(argv):
                    a = argv[i]
                    if a == "-o":
                        out, i = argv[i + 1], i + 2
                        continue
                    if a == "-tags":
                        p.tags, i = argv[i + 1], i + 2
                        continue
                    if not a.startswith("-"):
                        pkg = a
                    i += 1
                if not out or not pkg:
                    sys.exit(f"{connector}: can't read `{rest}`")
                p.builds[out.removeprefix("./")] = pkg
            elif argv[:2] == ["go", "test"]:
                flags, patterns, i = [], [], 2
                while i < len(argv):
                    a = argv[i]
                    if a == "-tags":
                        flags += argv[i:i + 2]
                        i += 2
                        continue
                    (patterns if a.startswith("./") else flags).append(a)
                    i += 1
                p.tests.append(GoTest(flags, patterns, dict(env)))
            elif argv[:2] in (["go", "install"], ["go", "mod"]):
                pass
            else:
                # apt packages, `go run` installers, anything else Go alone
                # doesn't reproduce.
                p.docker_only = True
    return p


def run(argv: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(argv, check=True, text=True, **kw)


def go_list(fmt: str, patterns: list[str], tags: str = "") -> list[list[str]]:
    argv = ["go", "list", "-f", fmt]
    if tags:
        argv += ["-tags", tags]
    out = run(argv + patterns, stdout=subprocess.PIPE).stdout
    return [line.split() for line in out.splitlines() if line.strip()]


def rel(dir: str) -> str:
    return os.path.relpath(dir, os.getcwd())


def cmd_mode(args):
    print("docker" if plan(args.connector).docker_only else "prebuilt")


def cmd_build(args):
    out = Path(args.out)
    plans = [plan(c) for c in args.connectors]
    plans = [p for p in plans if not p.docker_only]
    if not plans:
        return
    jobs = []  # (description, argv)
    manifests: dict[str, list[str]] = {}

    # One `go list` per tag set: each loads the module's package graph.
    listed: dict[str, list[list[str]]] = {}
    for tags in {p.tags for p in plans}:
        patterns = sorted({pat for p in plans if p.tags == tags for pat in p.builds.values()})
        listed[tags] = [f for f in go_list("{{.Name}} {{.ImportPath}} {{.Dir}}", patterns, tags) if f[0] == "main"]
    with_tests = go_list("{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}} {{.Dir}}{{end}}",
                         [f"./{p.connector}/..." for p in plans])

    def under(dir: str, pattern: str) -> bool:
        base = os.path.abspath(pattern.removesuffix("/..."))
        return dir == base or (pattern.endswith("/...") and dir.startswith(base + "/"))

    debian = None
    for p in plans:
        dst = out / p.connector
        shutil.rmtree(dst, ignore_errors=True)
        (dst / "ctx").mkdir(parents=True)
        (dst / "tests").mkdir()
        lines = manifests.setdefault(p.connector, [])

        for name, pattern in p.builds.items():
            mains = [f for f in listed[p.tags] if under(f[2], pattern)]
            if len(mains) != 1:
                sys.exit(f"{p.connector}: `go build {pattern}` matches {len(mains)} main packages")
            _, pkg, dir = mains[0]
            binary = dst / "ctx" / "builder" / name
            argv = ["go", "build", "-o", str(binary)]
            if p.tags:
                argv += ["-tags", p.tags]
            jobs.append((f"{p.connector}: build {name}", argv + [pkg]))
            lines.append(f"run {rel(dir)} {binary}")

        # Programs the connector's test harness scripts `go run`.
        for i, script in enumerate(sorted(Path("tests", p.connector).glob("*.sh"))):
            for target in re.findall(r"\bgo run (\S+)", script.read_text()):
                binary = dst / "tools" / f"{i}-{Path(target).stem}"
                jobs.append((f"{p.connector}: build {target}", ["go", "build", "-o", str(binary), target]))
                lines.append(f"run {rel(os.path.abspath(target))} {binary}")

        for src in p.copies:
            if src.startswith("/builder/"):
                name = src.removeprefix("/builder/")
                if name in p.builds:
                    continue
                if not Path(name).exists():
                    sys.exit(f"{p.connector}: runtime stage copies {src}, which the repo doesn't have")
                target = dst / "ctx" / "builder" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                (shutil.copytree if Path(name).is_dir() else shutil.copy2)(name, target)
            else:
                if debian is None:
                    debian = run(["docker", "create", "--platform", "linux/amd64", DEBIAN_IMAGE],
                                 stdout=subprocess.PIPE).stdout.strip()
                target = dst / "ctx" / src.lstrip("/")
                target.parent.mkdir(parents=True, exist_ok=True)
                run(["docker", "cp", "-L", f"{debian}:{src}", str(target)])

        # The test jobs' `go test` runs build without tags, like the
        # integration tests and the Dockerfiles' source connector tests.
        for i, (pkg, dir) in enumerate(f for f in with_tests if under(f[1], f"./{p.connector}/...")):
            binary = dst / "tests" / f"{i}.test"
            jobs.append((f"{p.connector}: test {pkg}",
                         ["go", "test", "-c", "-ldflags=-s -w", "-o", str(binary), pkg]))
            lines.append(f"test {rel(dir)} {binary}")

        script = ["set -euo pipefail"]
        if not p.connector.startswith("materialize-"):
            # tests/materialize/run.sh tests every package of a
            # materialization, without -short.
            for t in p.tests:
                own = [x for x in t.patterns if x.startswith(f"./{p.connector}/") or x == f"./{p.connector}"]
                if not own:
                    continue
                flags = [f for f in t.flags if f != "-v"]
                envs = [f"{k}={shlex.quote(v)}" for k, v in t.env.items()]
                script.append(shlex.join(["env", *envs, "go", "test", *flags, *own]) if envs
                              else shlex.join(["go", "test", *flags, *own]))
        (dst / "unit-tests.sh").write_text("\n".join(script) + "\n")

    if debian:
        run(["docker", "rm", debian], stdout=subprocess.DEVNULL)

    for c, lines in manifests.items():
        (out / c / "go.txt").write_text("\n".join(lines) + "\n")

    # Vet once for every connector, as `go test` does before testing.
    patterns = [f"./{p.connector}/..." for p in plans]
    jobs.append(("vet", ["go", "vet", *VET_ANALYZERS, *patterns]))

    failed = []
    with concurrent.futures.ThreadPoolExecutor(os.cpu_count()) as pool:
        futures = {pool.submit(subprocess.run, argv, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True): desc
                   for desc, argv in jobs}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            if r.returncode != 0:
                failed.append(futures[f])
                print(f"::group::FAILED {futures[f]}\n{r.stdout}::endgroup::", flush=True)
                print(f"::error::{futures[f]} failed")
            else:
                print(f"{futures[f]}", flush=True)
    if failed:
        sys.exit(1)


def cmd_helper_tests(args):
    groups: dict[tuple[str, ...], set[str]] = {}

    def add(flags, patterns):
        groups.setdefault(tuple(sorted(f for f in flags if f != "-v")), set()).update(patterns)

    connectors = [c for c in args.connectors if not plan(c).docker_only]
    for c in connectors:
        for t in plan(c).tests:
            other = [x for x in t.patterns if not (x.startswith(f"./{c}/") or x == f"./{c}")]
            if other:
                add(t.flags, other)
        if c.startswith("materialize-"):
            add([], MATERIALIZE_SHARED)

    # Every in-repo package the connectors depend on, besides their own.
    deps = go_list(
        "{{if and (not .ForTest) .Module (eq .Module.Path \"" + MODULE + "\")}}{{.ImportPath}}{{end}}",
        ["-deps", "-test"] + [f"./{c}/..." for c in connectors])
    own = tuple(f"{MODULE}/{c}" for c in connectors)
    pkgs = sorted({d[0] for d in deps
                   if not d[0].endswith(".test")
                   and not any(d[0] == o or d[0].startswith(o + "/") for o in own)})
    if pkgs:
        add(["-short"], pkgs)

    for flags, patterns in sorted(groups.items()):
        print(shlex.join(["go", "test", "-count=1", *flags, *sorted(patterns)]))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(required=True)
    m = sub.add_parser("mode")
    m.add_argument("connector")
    m.set_defaults(fn=cmd_mode)
    b = sub.add_parser("build")
    b.add_argument("--out", required=True)
    b.add_argument("connectors", nargs="+")
    b.set_defaults(fn=cmd_build)
    h = sub.add_parser("helper-tests")
    h.add_argument("connectors", nargs="+")
    h.set_defaults(fn=cmd_helper_tests)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
