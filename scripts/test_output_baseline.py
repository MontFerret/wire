"""Exercise the baseline driver with frozen fixture outputs instead of timed workloads."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
from io import StringIO
import json
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


DRIVER = Path(__file__).with_name("output-baseline.py")
CLI = "BenchmarkWireOutputDelivery"
REPLAY = "BenchmarkDebuggerOutputReplay"
CLI_CASES = ("64B/whole", "1MiB/whole", "8MiB/whole", "1MiB/256B")
WIRE_CASES = (*CLI_CASES, "1MiB/64KiB", "1MiB/4KiB")


def records(family, cases, samples=1, iterations=3, metrics="12.5 ns/op  8 MB/s  32 B/op  0 allocs/op"):
    return "".join(f"{family}/{case}-12\t{iterations}\t{metrics}\n" * samples for case in cases) + "PASS\n"


class DriverTests(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for side in ("before", "after"):
            directory = self.root / side
            directory.mkdir()
            hashes = {}
            for name in ("wire.test", "cli.test", "server.test"):
                data = name.encode()
                (directory / name).write_bytes(data)
                hashes[name] = hashlib.sha256(data).hexdigest()
            (directory / "binaries.json").write_text(json.dumps(hashes))

    def driver(self, phase, suite, respond, error=None):
        out = self.root / "before"
        args = [str(DRIVER), phase, str(out), "--suite", suite]
        if phase == "compare":
            tmp = TemporaryDirectory(dir=self.root)
            self.addCleanup(tmp.cleanup)
            args[2] = tmp.name
            args.extend(["--before", str(out), "--after", str(self.root / "after")])

        def emit(command, *, stdout, check, timeout, **kwargs):
            self.assertTrue(check)
            self.assertGreater(timeout, 0)
            stdout.write(respond(command))

        stderr = StringIO()
        with patch.object(sys, "argv", args), patch.object(subprocess, "run", side_effect=emit) as run, \
                redirect_stdout(StringIO()), redirect_stderr(stderr):
            if error is None:
                runpy.run_path(str(DRIVER))
            else:
                with self.assertRaises(SystemExit) as failure:
                    runpy.run_path(str(DRIVER))
                self.assertNotEqual(failure.exception.code, 0)
                self.assertIn(error, stderr.getvalue())
        return [call.args[0] for call in run.call_args_list]

    def test_complete_selected_timing_suite(self):
        for suite, family, cases in (("cli", CLI, CLI_CASES), ("wire", "BenchmarkExecutionOutputDelivery", WIRE_CASES)):
            with self.subTest(suite=suite):
                calls = self.driver("timing", suite, lambda _: records(family, cases, 10))
                self.assertEqual(len(calls), 1)
                self.assertEqual(Path(calls[0][0]).name, f"{suite}.test")
                self.assertIn("-test.count=10", calls[0])

    def test_timing_requires_complete_records(self):
        for output, reason in (
            ("PASS\n", "64B/whole: expected 10 samples, got 0"),
            (records(CLI, CLI_CASES[:-1], 10), "1MiB/256B: expected 10 samples, got 0"),
            (records(CLI, CLI_CASES, 9), "64B/whole: expected 10 samples, got 9"),
            (records(CLI, CLI_CASES, 10, metrics="1 ns/op 1 allocs/op"), "64B/whole: missing B/op"),
            (records(CLI, CLI_CASES, 10, metrics="1 ns/op 1 B/op"), "64B/whole: missing allocs/op"),
            (records(CLI, CLI_CASES, 10, metrics="1 ns/op 1 B/op 0 allocs/op 1 B/op"), "64B/whole: incomplete or duplicate metric"),
            (records(CLI, CLI_CASES, 10, metrics="NaN ns/op 1 B/op 0 allocs/op"), "64B/whole: invalid ns/op"),
            (records(CLI, CLI_CASES, 10, iterations=0), "64B/whole: invalid iteration count"),
        ):
            with self.subTest(reason=reason):
                self.driver("timing", "cli", lambda _: output, f"timing cli: {CLI}/{reason}")

    def test_compare_checks_each_side_and_sample(self):
        cases = ("small", "large")
        complete = records(REPLAY, cases)
        calls = self.driver("compare", "server", lambda _: complete)
        order = [Path(cmd[0]).parent.name for cmd in calls]
        self.assertEqual(order, ["before", "after", "after", "before"] * 5)
        for invalid, reason in (
            ("PASS\n", "small: expected 2 samples, got 1"),
            (records(REPLAY, ("small",)), "large: expected 2 samples, got 1"),
            (records(REPLAY, cases, 2), "small: expected 2 samples, got 3"),
            (records(REPLAY, cases, metrics="1 ns/op 0 allocs/op"), "small: missing B/op"),
        ):
            with self.subTest(reason=reason):
                after = iter((complete, invalid))
                self.driver("compare", "server", lambda cmd: next(after) if Path(cmd[0]).parent.name == "after" else complete,
                            f"compare server after sample 2: {REPLAY}/{reason}")

        self.driver("compare", "server", lambda cmd: records(REPLAY, ("small", "medium"))
                    if Path(cmd[0]).parent.name == "before" else complete,
                    f"compare server before sample 1: unexpected benchmark {REPLAY}/medium")

    def test_diagnostics_require_passed_tests_and_subtests(self):
        tests = {
            "TestOutputDeliveryFrames": (*WIRE_CASES, "uneven"),
            "TestOutputDeliveryFailures": ("terminal", "cleanup"),
            "TestOutputDeliverySlowConsumer": ("resume", "cancel"),
        }
        lines = [f"--- PASS: {name} (0.01s)" for name in tests]
        lines.extend(f"    --- PASS: {name}/{case} (0.01s)" for name, cases in tests.items() for case in cases)
        output = "\n".join(lines) + "\nPASS\n"
        self.driver("diagnostics", "wire", lambda _: output)
        self.driver("diagnostics", "wire", lambda _: "testing: warning: no tests to run\nPASS\n",
                    "diagnostics wire: TestOutputDeliveryFailures: expected one PASS, got 0")
        name = "TestOutputDeliveryFrames/uneven"
        skipped = output.replace("PASS: " + name, "SKIP: " + name)
        self.driver("diagnostics", "wire", lambda _: skipped, f"diagnostics wire: {name}: SKIP")

    def test_profiles_use_the_selected_case_and_its_fixed_iterations(self):
        seen = []

        def respond(cmd):
            if cmd[0] == "go":
                metric = cmd[3].removeprefix("-")
                return f"Type: {metric}\nShowing nodes accounting for 123, 80% of 150 total\n  123 80% 80% 123 80% output.writer\n"
            selection = next(arg for arg in cmd if arg.startswith("-test.bench="))
            case = "/".join(part.strip("^$") for part in selection.split("/")[1:])
            iterations = 10 if case == "1MiB/256B" else 100
            self.assertIn(f"-test.benchtime={iterations}x", cmd)
            self.assertIn("-test.count=1", cmd)
            profile = next(arg.split("=", 1)[1] for arg in cmd if arg.startswith("-test.memprofile="))
            Path(profile).write_bytes(b"profile fixture")
            seen.append(case)
            return records(CLI, (case,), iterations=iterations, metrics="180 ns/op")

        self.driver("profiles", "cli", respond)
        self.assertEqual(seen, ["64B/whole", "1MiB/whole", "1MiB/256B"])

    def test_profiles_reject_missing_work_and_unusable_profiles(self):
        case = CLI_CASES[0]
        work = f"profiles cli: {CLI}/{case}"
        self.driver("profiles", "cli", lambda _: "PASS\n", f"{work}: expected 1 samples, got 0")
        self.driver("profiles", "cli", lambda _: records(CLI, (case,), iterations=10),
                    f"{work}: expected 100 iterations, got 10")
        profile = self.root / "before" / "cli-64B-whole.pprof"
        profile.write_bytes(b"stale")
        output = records(CLI, (case,), iterations=100)
        self.driver("profiles", "cli", lambda _: output, f"profiles cli: {case}: missing or empty profile")

        def unreadable(cmd):
            if cmd[0] == "go":
                raise subprocess.CalledProcessError(1, cmd)
            profile.write_bytes(b"unreadable")
            return output

        self.driver("profiles", "cli", unreadable, "cli-64B-whole-alloc_objects-flat.txt")

        def empty_space_report(cmd):
            if cmd[0] == "go":
                if "-alloc_space" in cmd:
                    return "Type: alloc_space\n      flat flat% sum% cum cum%\n"
                return "Type: alloc_objects\n  123 80% 80% 123 80% output.writer\n"
            profile.write_bytes(b"profile fixture")
            return output

        self.driver("profiles", "cli", empty_space_report, f"profiles cli: {case}: pprof reported no alloc_space samples")


if __name__ == "__main__":
    unittest.main()
