#!/usr/bin/env python3
"""Freeze and measure streaming fixtures; keep all generated files outside Git.

Usage: python3 scripts/output-baseline.py PHASE OUTPUT_DIRECTORY [CLI_DIRECTORY]
PHASE is build, timing, profiles, diagnostics, or compare.
For compare, add --before FROZEN_DIRECTORY --after FROZEN_DIRECTORY.
Comparisons alternate ten ordinary samples of each frozen binary, including replay.
Use --suite to select one workload: timing and profiles support wire and cli,
diagnostics supports wire, and compare also supports server replay.
The CLI directory defaults to ../cli. Go and Python 3 must be on PATH.
"""

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import tarfile


WIRE = Path(__file__).resolve().parent.parent
FAMILIES = {"wire": "BenchmarkExecutionOutputDelivery", "cli": "BenchmarkWireOutputDelivery",
            "server": "BenchmarkDebuggerOutputReplay"}
WHOLE_CASES = ("64B/whole", "1MiB/whole", "8MiB/whole")
CASES = {"wire": (*WHOLE_CASES, "1MiB/64KiB", "1MiB/4KiB", "1MiB/256B"),
         "cli": (*WHOLE_CASES, "1MiB/256B"), "server": ("small", "large")}
SAMPLES = 10
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("phase", choices=("build", "timing", "profiles", "diagnostics", "compare"))
parser.add_argument("output_directory", type=Path)
parser.add_argument("cli_directory", type=Path, nargs="?", default=WIRE.parent / "cli")
parser.add_argument("--before", type=Path)
parser.add_argument("--after", type=Path)
parser.add_argument("--suite", choices=("wire", "cli", "server"))
arguments = parser.parse_args()
PHASE = arguments.phase
OUT = arguments.output_directory.resolve()
CLI = arguments.cli_directory.resolve()
suites = {"build": (), "timing": ("wire", "cli"), "profiles": ("wire", "cli"),
          "diagnostics": ("wire",), "compare": tuple(FAMILIES)}[PHASE]
if arguments.suite is not None:
    if arguments.suite not in suites:
        parser.error(f"{PHASE} does not support --suite {arguments.suite}")
    suites = (arguments.suite,)
if any(OUT == repo or repo in OUT.parents for repo in (WIRE, CLI)):
    parser.error("output_directory must be outside both repositories")
OUT.mkdir(parents=True, exist_ok=True)
ENV = dict(os.environ, GOMAXPROCS="12", GOWORK="off")
for setting in ("GOFLAGS", "GOGC", "GOMEMLIMIT", "GODEBUG"):
    ENV.pop(setting, None)


def verify_binaries(directory):
    hashes = json.loads((directory / "binaries.json").read_text())
    if not {"wire.test", "cli.test"}.issubset(hashes):
        parser.error("frozen manifest must include wire.test and cli.test")
    for name, expected in hashes.items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != expected:
            parser.error(f"frozen binary changed: {name}")
    return hashes


if PHASE == "compare":
    if arguments.before is None or arguments.after is None:
        parser.error("compare requires --before and --after")
    variants = {"before": arguments.before.resolve(), "after": arguments.after.resolve()}
    provenance = {}
    for label, directory in variants.items():
        hashes = verify_binaries(directory)
        if "server.test" not in hashes:
            parser.error("compare requires a build including server.test")
        provenance[label] = {"directory": str(directory), "binaries": hashes}
    if list(OUT.glob("*-before.txt")) or list(OUT.glob("*-after.txt")):
        parser.error("comparison output already exists; use a fresh directory")
    (OUT / "comparison.json").write_text(json.dumps(provenance, indent=2) + "\n")
elif PHASE != "build":
    verify_binaries(OUT)


def run(command, destination, cwd=WIRE, timeout=300, append=False):
    """Record the exact invocation and fail on incomplete or failed runs."""
    with (OUT / "commands.txt").open("a") as commands:
        commands.write(f"cd {shlex.quote(str(cwd))} && {shlex.join(map(str, command))}\n")
    print(f"{PHASE}: {destination}", flush=True)
    path = OUT / destination
    with path.open("a" if append else "w") as output:
        try:
            subprocess.run(list(map(str, command)), cwd=cwd, env=ENV, stdout=output,
                           stderr=subprocess.STDOUT, check=True, timeout=timeout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as err:
            parser.exit(1, f"{PHASE}: {path}: {err}\n")
    return path


def check_measurements(path, suite, cases, samples, *, iterations=None, side=None):
    """Check Go benchmark records only. Profile reports and test logs have other formats."""
    work = f"{PHASE} {suite}" if side is None else f"{PHASE} {suite} {side} sample {samples}"
    counts = {f"{FAMILIES[suite]}/{case}": 0 for case in cases}
    required = ("ns/op",) if iterations is not None else ("ns/op", "B/op", "allocs/op")
    for line in path.read_text().splitlines():
        if not line.startswith("Benchmark"):
            continue
        fields = line.split()
        name = re.sub(r"-[0-9]+$", "", fields[0])
        if name not in counts:
            parser.error(f"{work}: unexpected benchmark {name}")
        if len(fields) < 2 or not re.fullmatch(r"[1-9][0-9]*", fields[1]):
            parser.error(f"{work}: {name}: invalid iteration count")
        if iterations is not None and int(fields[1]) != iterations:
            parser.error(f"{work}: {name}: expected {iterations} iterations, got {fields[1]}")
        metrics = dict(zip(fields[3::2], fields[2::2]))
        if len(metrics) * 2 != len(fields) - 2:
            parser.error(f"{work}: {name}: incomplete or duplicate metric in {line}")
        for unit in required:
            if unit not in metrics:
                parser.error(f"{work}: {name}: missing {unit}")
            try:
                value = float(metrics[unit])
            except ValueError:
                parser.error(f"{work}: {name}: invalid {unit} value {metrics[unit]}")
            if not math.isfinite(value) or value < 0:
                parser.error(f"{work}: {name}: invalid {unit} value {metrics[unit]}")
        counts[name] += 1
    for name, count in counts.items():
        if count != samples:
            parser.error(f"{work}: {name}: expected {samples} samples, got {count}")


def check_diagnostics(path):
    tests = {"TestOutputDeliveryFrames": (*CASES["wire"], "uneven"),
             "TestOutputDeliveryFailures": ("terminal", "cleanup"),
             "TestOutputDeliverySlowConsumer": ("resume", "cancel")}
    expected = set(tests)
    expected.update(f"{parent}/{case}" for parent, cases in tests.items() for case in cases)
    passed = Counter()
    for result, name in re.findall(r"^[ \t]*--- (PASS|SKIP|FAIL): (\S+)", path.read_text(), re.MULTILINE):
        if name in expected:
            if result != "PASS":
                parser.error(f"diagnostics wire: {name}: {result}")
            passed[name] += 1
    for name in sorted(expected):
        if passed[name] != 1:
            parser.error(f"diagnostics wire: {name}: expected one PASS, got {passed[name]}")


def check_profile_report(path, suite, case, metric):
    # A readable but empty profile can produce a successful pprof -top header.
    lines = path.read_text().splitlines()
    has_nodes = any(len(row) >= 6 and row[1].endswith("%") and row[2].endswith("%")
                    for row in map(str.split, lines))
    if f"Type: {metric}" not in lines or not has_nodes:
        parser.error(f"profiles {suite}: {case}: pprof reported no {metric} samples in {path}")


def snapshot(repo, label):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo)

    (OUT / f"{label}-revision.txt").write_bytes(git("rev-parse", "HEAD"))
    (OUT / f"{label}-status.txt").write_bytes(git("status", "--short"))
    (OUT / f"{label}-dirty.patch").write_bytes(git("diff", "--binary", "HEAD"))
    paths = set(git("diff", "--name-only", "HEAD", "-z").decode().split("\0"))
    paths.update(git("ls-files", "--others", "--exclude-standard", "-z").decode().split("\0"))
    manifest = {}
    with tarfile.open(OUT / f"{label}-dirty.tar.gz", "w:gz") as archive:
        for name in sorted(paths - {""}):
            path = repo / name
            if path.is_file():
                archive.add(path, arcname=name)
                manifest[name] = hashlib.sha256(path.read_bytes()).hexdigest()
            else:
                manifest[name] = "deleted"
    (OUT / f"{label}-dirty-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if PHASE == "build":
    (OUT / "environment.txt").write_text(
        f"Platform={platform.platform()}\n"
        "GOMAXPROCS=12\nGOWORK=off\nGOFLAGS,GOGC,GOMEMLIMIT,GODEBUG unset\n"
        "Both endpoints in-process; plaintext loopback TCP; default gRPC buffers/windows.\n"
    )
    for label, repo in (("wire", WIRE), ("cli", CLI)):
        snapshot(repo, label)
    run(["go", "version"], "go-version.txt")
    run(["go", "env", "-json", "GOOS", "GOARCH", "GOHOSTOS", "GOHOSTARCH", "GOTOOLCHAIN", "GOEXPERIMENT", "CGO_ENABLED"], "go-env.json")
    run(["go", "list", "-mod=readonly", "-m", "-json", "all"], "wire-modules.json")
    run(["go", "test", "-mod=readonly", "-c", "-o", OUT / "wire.test", "./test/integration"], "wire-build.txt")
    run(["go", "test", "-mod=readonly", "-c", "-o", OUT / "server.test", "./server"], "server-build.txt")
    shutil.copyfile(CLI / "go.mod", OUT / "cli.mod")
    shutil.copyfile(CLI / "go.sum", OUT / "cli.sum")
    run(["go", "mod", "edit", f"-modfile={OUT / 'cli.mod'}", f"-replace=github.com/MontFerret/wire={WIRE}"], "cli-modfile.txt", CLI)
    run(["go", "list", "-mod=readonly", f"-modfile={OUT / 'cli.mod'}", "-m", "-json", "all"], "cli-modules.json", CLI)
    run(["go", "test", "-mod=readonly", f"-modfile={OUT / 'cli.mod'}", "-c", "-o", OUT / "cli.test", "./internal/output"], "cli-build.txt", CLI)
    hashes = {name: hashlib.sha256((OUT / name).read_bytes()).hexdigest() for name in ("wire.test", "cli.test", "server.test")}
    (OUT / "binaries.json").write_text(json.dumps(hashes, indent=2) + "\n")
elif PHASE == "timing":
    for suite in suites:
        path = run([OUT / f"{suite}.test", "-test.run=^$", f"-test.bench=^{FAMILIES[suite]}$", "-test.benchmem",
                    "-test.benchtime=1s", f"-test.count={SAMPLES}", "-test.timeout=5m"], f"{suite}-timing.txt", timeout=330)
        check_measurements(path, suite, CASES[suite], SAMPLES)
elif PHASE == "profiles":
    for suite in suites:
        binary = OUT / f"{suite}.test"
        for case, iterations in (("64B/whole", 100), ("1MiB/whole", 100), ("1MiB/256B", 10)):
            stem = f"{suite}-{case.replace('/', '-')}"
            profile = OUT / f"{stem}.pprof"
            profile.unlink(missing_ok=True)  # A previous run cannot supply this profile.
            selection = "^" + FAMILIES[suite] + "$/" + "/".join(f"^{part}$" for part in case.split("/"))
            path = run([binary, "-test.run=^$", f"-test.bench={selection}", f"-test.benchtime={iterations}x",
                        "-test.count=1", "-test.memprofilerate=1", f"-test.memprofile={profile}", "-test.timeout=2m"],
                       f"{stem}-profile-run.txt", timeout=150)
            check_measurements(path, suite, (case,), 1, iterations=iterations)
            if not profile.is_file() or profile.stat().st_size == 0:
                parser.error(f"profiles {suite}: {case}: missing or empty profile {profile}")
            for metric in ("alloc_objects", "alloc_space"):
                for order in ("flat", "cum"):
                    args = ["go", "tool", "pprof", f"-{metric}", "-top", "-nodecount=30"]
                    if order == "cum":
                        args.append("-cum")
                    report = run([*args, binary, profile], f"{stem}-{metric}-{order}.txt")
                    check_profile_report(report, suite, case, metric)
            run(["go", "tool", "pprof", "-alloc_objects", "-list=outputstream.Chunks|outputWriter.*consume|executionDelivery.*send", binary, profile], f"{stem}-framing-objects.txt")
            for metric in ("alloc_objects", "alloc_space"):
                run(["go", "tool", "pprof", f"-{metric}", "-list=internal/output.Write|Benchmark.*Delivery.func1.1|openOutput|newExecutionDelivery|core.newExecution", binary, profile], f"{stem}-owners-{metric}.txt")
elif PHASE == "diagnostics":
    path = run([OUT / "wire.test", "-test.run=^TestOutputDelivery", "-test.v", "-test.count=1", "-test.timeout=1m"],
               "wire-diagnostics.txt", timeout=75)
    check_diagnostics(path)
elif PHASE == "compare":
    for suite in suites:
        for sample in range(SAMPLES):
            order = ("before", "after") if sample % 2 == 0 else ("after", "before")
            for label in order:
                path = run([variants[label] / f"{suite}.test", "-test.run=^$",
                            f"-test.bench=^{FAMILIES[suite]}$", "-test.benchmem", "-test.benchtime=1s",
                            "-test.count=1", "-test.timeout=2m"], f"{suite}-{label}.txt", timeout=150, append=True)
                # Checking each append ensures this invocation added one of every case.
                check_measurements(path, suite, CASES[suite], sample + 1, side=label)
