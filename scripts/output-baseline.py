#!/usr/bin/env python3
"""Freeze and measure streaming fixtures; keep all generated files outside Git.

Usage: python3 scripts/output-baseline.py PHASE OUTPUT_DIRECTORY [CLI_DIRECTORY]
PHASE is build, timing, profiles, or diagnostics. Run phases serially in that order.
The CLI directory defaults to ../cli. Go and Python 3 must be on PATH.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tarfile


WIRE = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("phase", choices=("build", "timing", "profiles", "diagnostics"))
parser.add_argument("output_directory", type=Path)
parser.add_argument("cli_directory", type=Path, nargs="?", default=WIRE.parent / "cli")
arguments = parser.parse_args()
PHASE = arguments.phase
OUT = arguments.output_directory.resolve()
CLI = arguments.cli_directory.resolve()
if any(OUT == repo or repo in OUT.parents for repo in (WIRE, CLI)):
    parser.error("output_directory must be outside both repositories")
OUT.mkdir(parents=True, exist_ok=True)
ENV = dict(os.environ, GOMAXPROCS="12", GOWORK="off")
for setting in ("GOFLAGS", "GOGC", "GOMEMLIMIT", "GODEBUG"):
    ENV.pop(setting, None)

if PHASE != "build":
    hashes = json.loads((OUT / "binaries.json").read_text())
    for name in ("wire.test", "cli.test"):
        if hashlib.sha256((OUT / name).read_bytes()).hexdigest() != hashes[name]:
            parser.error(f"frozen binary changed: {name}")


def run(command, destination, cwd=WIRE, timeout=300):
    """Record the exact invocation and fail on incomplete or failed runs."""
    with (OUT / "commands.txt").open("a") as commands:
        commands.write(f"cd {shlex.quote(str(cwd))} && {shlex.join(map(str, command))}\n")
    print(f"{PHASE}: {destination}", flush=True)
    with (OUT / destination).open("w") as output:
        subprocess.run(list(map(str, command)), cwd=cwd, env=ENV, stdout=output,
                       stderr=subprocess.STDOUT, check=True, timeout=timeout)


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
        "GOMAXPROCS=12\nGOWORK=off\nGOFLAGS,GOGC,GOMEMLIMIT,GODEBUG unset\n"
        "Both endpoints in-process; plaintext loopback TCP; default gRPC buffers/windows.\n"
    )
    for label, repo in (("wire", WIRE), ("cli", CLI)):
        snapshot(repo, label)
    run(["go", "version"], "go-version.txt")
    run(["go", "env", "-json", "GOOS", "GOARCH", "GOHOSTOS", "GOHOSTARCH", "GOTOOLCHAIN", "GOEXPERIMENT", "CGO_ENABLED"], "go-env.json")
    run(["go", "list", "-mod=readonly", "-m", "-json", "all"], "wire-modules.json")
    run(["go", "test", "-mod=readonly", "-c", "-o", OUT / "wire.test", "./test/integration"], "wire-build.txt")
    shutil.copyfile(CLI / "go.mod", OUT / "cli.mod")
    shutil.copyfile(CLI / "go.sum", OUT / "cli.sum")
    run(["go", "mod", "edit", f"-modfile={OUT / 'cli.mod'}", f"-replace=github.com/MontFerret/wire={WIRE}"], "cli-modfile.txt", CLI)
    run(["go", "list", "-mod=readonly", f"-modfile={OUT / 'cli.mod'}", "-m", "-json", "all"], "cli-modules.json", CLI)
    run(["go", "test", "-mod=readonly", f"-modfile={OUT / 'cli.mod'}", "-c", "-o", OUT / "cli.test", "./internal/output"], "cli-build.txt", CLI)
    hashes = {name: hashlib.sha256((OUT / name).read_bytes()).hexdigest() for name in ("wire.test", "cli.test")}
    (OUT / "binaries.json").write_text(json.dumps(hashes, indent=2) + "\n")
elif PHASE == "timing":
    for label, name in (("wire", "BenchmarkExecutionOutputDelivery"), ("cli", "BenchmarkWireOutputDelivery")):
        run([OUT / f"{label}.test", "-test.run=^$", f"-test.bench=^{name}$", "-test.benchmem", "-test.benchtime=1s", "-test.count=10", "-test.timeout=5m"], f"{label}-timing.txt", timeout=330)
elif PHASE == "profiles":
    for label, name in (("wire", "BenchmarkExecutionOutputDelivery"), ("cli", "BenchmarkWireOutputDelivery")):
        binary = OUT / f"{label}.test"
        for case, iterations in (("64B/whole", 100), ("1MiB/whole", 100), ("1MiB/256B", 10)):
            stem = f"{label}-{case.replace('/', '-')}"
            profile = OUT / f"{stem}.pprof"
            selection = "^" + name + "$/" + "/".join(f"^{part}$" for part in case.split("/"))
            run([binary, "-test.run=^$", f"-test.bench={selection}", f"-test.benchtime={iterations}x", "-test.count=1", "-test.memprofilerate=1", f"-test.memprofile={profile}", "-test.timeout=2m"], f"{stem}-profile-run.txt", timeout=150)
            for metric in ("alloc_objects", "alloc_space"):
                for order in ("flat", "cum"):
                    args = ["go", "tool", "pprof", f"-{metric}", "-top", "-nodecount=30"]
                    if order == "cum":
                        args.append("-cum")
                    run([*args, binary, profile], f"{stem}-{metric}-{order}.txt")
            run(["go", "tool", "pprof", "-alloc_objects", "-list=outputstream.Chunks|outputWriter.*consume|executionDelivery.*send", binary, profile], f"{stem}-framing-objects.txt")
            for metric in ("alloc_objects", "alloc_space"):
                run(["go", "tool", "pprof", f"-{metric}", "-list=internal/output.Write|Benchmark.*Delivery.func1.1|openOutput|newExecutionDelivery|core.newExecution", binary, profile], f"{stem}-owners-{metric}.txt")
elif PHASE == "diagnostics":
    run([OUT / "wire.test", "-test.run=^TestOutputDelivery", "-test.v", "-test.count=1", "-test.timeout=1m"], "wire-diagnostics.txt", timeout=75)
