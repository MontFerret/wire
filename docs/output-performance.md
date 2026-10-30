# Alpha.21 output measurements

The historical `BenchmarkExecutionOutput` retained every hosted call and scanned
the growing call history. Its RPC wrapper also retained unary call order.
`BenchmarkExecutionOutputDelivery` uses a different fixture and transport, so
its results cannot be compared directly with the execution-output timings and
allocations below.

Measured on 2026-10-28 on an Apple M2 Max, darwin/arm64, Go 1.27.3, with
GOMAXPROCS fixed at 12. These are in-process gRPC benchmarks, not network
service-level guarantees. All comparisons report median ns/op, B/op, and
allocs/op. The compared benchmark workloads and fixtures did not change.

## Public execution comparison

The baseline is the alpha.21 implementation at `d9d01b2`, before these
optimizations. The core durable-session benchmark is excluded from all
comparisons because its fixture changed during migration. It remains available
as a standalone diagnostic, but does not support production performance claims.

The primary comparison uses frozen baseline and candidate test binaries. Ten
one-second samples per binary ran serially in alternating before/after order,
reversing the order every pair to reduce timing drift. `benchstat` reports:

| Public durable session | Before | After | Change |
| --- | ---: | ---: | ---: |
| ns/op | 91,752 | 90,273 | -1.61%, p=0.020 |
| B/op | 40,534.5 | 38,993 | -3.80%, p<0.001 |
| allocs/op | 662 | 636 | -3.93%, p<0.001 |

An earlier sequential ten-sample batch measured 94,765 → 92,638.5 ns/op,
40,366.5 → 38,821 B/op, and 661 → 636 allocs/op. Its latency change was -2.24%.
Debugger timings also moved despite unchanged debugger code, which prompted the
alternating confirmation above. The smaller confirmed latency gain is the primary
result; allocation savings are more stable than timing.

The historical alpha.20 reference was 81,101 ns/op, 35,595 B/op, and 568 allocs/op
(medians of three runs). Those older samples are not a fresh statistical
comparison. The optimization has not established parity with alpha.20.

## Changes measured separately

- Caching the normal-output chunk budget reduced 661 → 658 allocs/op. Its timing
  difference was not significant (p=0.105).
- Reusing the client's existing output-lifetime callback and constructing one
  consumption context reduced 658 → 655 allocs/op. Timing was unchanged (p=0.739).
- Consolidating server contexts and using the execution's existing producer with
  one dedicated sender reduced 655 → 636 allocs/op. These changes are coupled:
  cancellation-aware waits require a handoff that remains safe after handler return.

Each retained step reduced allocations without a meaningful measured regression.
The final sender also retains its actual transport failure, independently of the
cancellation it triggers. Cancellation does not wait for blocked network sends or
hosted cleanup, and successful delivery still waits for the terminal Send.

## Small and large output guards

These are the original sequential ten-sample baseline/candidate batches, with
one-second samples and no concurrent tests or builds. Values are medians; halves
arise from the even sample count.

| Benchmark | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Execution Consume, 64 B | 112,548.5 | 107,297.5 | 44,233 | 42,148 | 693 | 660 |
| Execution Consume, 8 MiB | 3,297,286 | 3,283,640.5 | 18,673,746.5 | 18,752,403.5 | 3,869.5 | 3,827.5 |
| Debugger replay, 64 B | 42,814.5 | 41,860.5 | 16,832.5 | 16,833 | 282 | 282 |
| Debugger replay, 8 MiB | 4,289,369.5 | 4,212,724 | 34,362,317.5 | 34,396,409.5 | 3,548 | 3,532.5 |

The 64-byte execution batch showed -4.67% time and -4.76% allocations. Its timing
is subject to the drift observed above. The 8 MiB execution latency and allocation
count changes were not significant (p=0.218 and p=0.912). Its B/op increased 0.42%
(p=0.007): about 77 KiB across roughly 18 MiB of cumulative allocation. Both versions
copy the same payload bytes; gRPC pooling and transport scheduling affect total
allocation. The retained handoff bound is tested separately below. No material
large-output regression was observed.

Debugger replay is a regression guard, not an optimization claim: its code and
ownership were unchanged. Neither replay size showed a significant B/op or
allocation-count change. Its timing shifts demonstrate why the primary execution
comparison was repeated in alternating order.

Execution benchmarks discard borrowed chunks. Debugger replay validates raw framed
content and excludes the public client's materialized accumulator. B/op measures
cumulative allocation, not peak or retained memory.

## Reproducing the comparison

Build and preserve binaries before editing, then repeat for the candidate:

```sh
go test -c -o /tmp/wire-server-before.test ./server
go test -c -o /tmp/wire-integration-before.test ./test/integration
```

Run each workload serially, substituting the corresponding candidate binary for
its after measurement:

```sh
GOMAXPROCS=12 /tmp/wire-server-before.test -test.run '^$' \
  -test.bench '^BenchmarkRuntimeAdapterDurableSession$' \
  -test.benchmem -test.benchtime=1s -test.count=10
GOMAXPROCS=12 /tmp/wire-integration-before.test -test.run '^$' \
  -test.bench '^BenchmarkExecutionOutput$' \
  -test.benchmem -test.benchtime=1s -test.count=10
GOMAXPROCS=12 /tmp/wire-server-before.test -test.run '^$' \
  -test.bench '^BenchmarkDebuggerOutputReplay$' \
  -test.benchmem -test.benchtime=1s -test.count=10
benchstat before.txt after.txt
```

For the primary confirmation, use `-test.count=1` in ten alternating pairs and
append each binary's samples to its own file. Reverse pair order on even pairs.
The comparison tool was `golang.org/x/perf/cmd/benchstat` at
`v0.0.0-20260813145340-fd4a688df892`; it is not a project dependency.

## Bounded buffering and cancellation

`TestExecutionOutputBlockedSendRetainsCleanupOwner` invokes the actual normal
output RPC handler, stalls Send and hosted Close independently, and verifies
handler return for request cancellation, execution cancellation, disconnect, and
shutdown. It permits at most two producer chunks, checks exactly-once Close, and
verifies release remains pending until hosted cleanup settles.

Additional owning-layer tests cover delayed preparation, header/chunk/terminal
send failures, terminal-send acknowledgment, request values and deadlines, and
concurrent cached-budget reads with small message limits. Public integration tests
cover both deadline orderings, deadline error identity, unread output, competing
consumers, callback errors/panics, Close races, cleanup failures, and session reuse.
The root and native race suites pass.

These checks establish Wire's bounded normal-output handoff independently of
benchmark allocation totals. gRPC has its own flow-control and transport buffers.
Hosted runtimes may materialize output; Collect and debugger content deliberately
materialize delivered bytes. There is no aggregate output-size limit.
