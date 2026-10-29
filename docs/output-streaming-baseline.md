# Streaming allocation baseline

This baseline measures complete normal-output delivery and the CLI's shared
streaming writer. It changes no production framing, copying, ownership, codecs,
message limits, or chunk policy. Historical results remain in
[the original report](output-performance.md).

## Scope and fixture audit

Wire's `BenchmarkExecutionOutputDelivery` includes `Runtime.Run`, receiving the
metadata header, consuming every byte through `api.Output.Consume`, receiving the
terminal outcome, and execution/reference release. An explicit `Close` checks the
retained cleanup result. Each iteration checks its byte count and both errors.
The hosted runtime returns a fresh lightweight output handle borrowing an
immutable deterministic payload. It performs no query evaluation or encoding.

The CLI's `BenchmarkWireOutputDelivery` includes the same execution followed by
the actual `internal/output.Write` used by the run command, with newline disabled.
Its writer counts and discards bytes, and checks both returned errors. This is
shared-writer coverage, excluding CLI runtime selection, source/parameter
adaptation, native evaluation/encoding, Cobra, and process startup. The existing
host fixture constructs a native engine during setup, but the benchmark replaces
its Run method with the hosted output fixture.

Both endpoints run inside one process over plaintext loopback TCP, using the
public client/server, default message limits and gRPC flow control. The 64 KiB
production chunk cap is unchanged. Connection establishment, deterministic
payload creation, and server shutdown are outside timing. Outputs and per-result
cleanup remain inside timing. Neither benchmark calls Collect, ReadAll, or builds
an accumulating output buffer. Counting sinks are reused between iterations;
diagnostic counters and middleware are absent from timed/profiled runs.

The old `BenchmarkExecutionOutput` already called Consume, whose normal path
settles execution cleanup; it was not merely timing a lazy handle. However, its
runtime recorder retained each call and scanned all previous calls on every
record, while its fault wrapper retained unary call order. It also used bufconn
and a fixture-wide ten-second invocation context. The replacement bypasses both
recorders, uses the benchmark lifetime for invocations, checks byte counts and
Close errors, and has a new benchmark name. The original 64 B/8 MiB sizes remain,
but a direct numerical comparison would mix fixture and transport changes.

## Reproduction and provenance

Measurements use Go 1.27.3 on Apple M2 Max, macOS 26.7.1 (25G241), darwin/arm64,
GOMAXPROCS=12. GOWORK is off; GOFLAGS, GOGC, GOMEMLIMIT, and GODEBUG are unset.
There is no explicit GC or
allocation sampling override in timing runs. Both builds use `-mod=readonly`.

Wire production revision: `a7c7cb8fd89bf1daf53729c0b46104aedfe4b142`.
CLI base revision: `46429191ec0c8299c2cfede6b3cd483cf2685b1c`, with its existing
uncommitted streaming migration and the benchmark-only additions. The CLI's
declared Wire alpha.3 resolves to `53967b161c338009d84193b3fb8c876f733d1c98`;
these measurements instead replace it with the current local Wire checkout in a
temporary modfile. Original module files and vendor directories remain untouched.

The CLI benchmark-only companion commit is
`4170c54de287f24f8b320b0c2bce8d963aa91f2b`; it contains only the three new test
files. The CLI migration is a prerequisite, not part of that commit.
Local frozen binaries, binary profiles, source patches, source snapshots and
dirty-file hash manifests are retained under
`/tmp/wire-streaming-baseline-20261029/frozen`; the pre-task CLI snapshot is in its
parent directory. These temporary artifacts must be preserved separately for
exact reproduction until the CLI migration is committed. Checked-in raw output
and provenance identify the measured source state without including that migration.

From the Wire repository, with the CLI checkout in the sibling directory:

```sh
python3 scripts/output-baseline.py build /tmp/wire-streaming-baseline
python3 scripts/output-baseline.py timing /tmp/wire-streaming-baseline
python3 scripts/output-baseline.py profiles /tmp/wire-streaming-baseline
python3 scripts/output-baseline.py diagnostics /tmp/wire-streaming-baseline
```

The script freezes binaries before running ten ordinary one-second samples per
case, serially. Do not run builds, profiles, tests, or unrelated benchmarks
concurrently with measurement. It records commands and environment and stops on
failure. For future revision comparisons, freeze equivalent binaries and alternate
before/after one-sample runs, reversing order on alternate pairs. Raw timing files
are directly consumable by benchstat. No historical CLI percentages are reproduced
or claimed here.

Dependency versions: Unified API alpha.21, gRPC v1.84.0 and protobuf v1.36.12 in
both binaries; CLI native Ferret alpha.58. Wire uses x/net v0.57.0, x/sys v0.47.0
and x/text v0.40.0; CLI uses v0.59.0, v0.48.0 and v0.42.0 respectively. The full
module selections, binary hashes and dirty-source hashes are in
[provenance.json](benchmarks/2026-10-29-streaming/provenance.json).

Evidence: [Wire samples](benchmarks/2026-10-29-streaming/wire-timing.txt),
[CLI samples](benchmarks/2026-10-29-streaming/cli-timing.txt),
[commands](benchmarks/2026-10-29-streaming/commands.txt),
[diagnostics](benchmarks/2026-10-29-streaming/wire-diagnostics.txt).

## Results

All values are medians of ten ordinary samples; timing ranges show the full
observed minimum–maximum, not confidence intervals. MB/s is decimal payload
throughput, excluding protocol overhead. B/op is cumulative allocation by both
endpoints, **not peak live or retained memory**.

| Consumer | Payload / callback | Data frames | µs/op (range) | B/op | allocs/op | MB/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| wire | 64B/whole | 1 | 228.9 (224.4–243.4) | 45,133.0 | 652.0 | 0.28 |
| wire | 1MiB/whole | 16 | 782.8 (742.3–825.5) | 3,031,340.5 | 987.0 | 1,339.51 |
| wire | 8MiB/whole | 128 | 3,551.2 (3,446.6–3,775.7) | 19,136,596.5 | 3,360.5 | 2,362.26 |
| wire | 1MiB/64KiB | 16 | 760.4 (750.4–777.0) | 3,017,606.5 | 985.0 | 1,378.90 |
| wire | 1MiB/4KiB | 256 | 1,408.7 (1,392.7–1,516.7) | 2,435,410.5 | 4,499.5 | 744.34 |
| wire | 1MiB/256B | 4,096 | 8,149.5 (8,058.4–8,780.5) | 7,113,167.0 | 80,002.0 | 128.66 |
| cli | 64B/whole | 1 | 231.3 (227.4–233.4) | 46,091.0 | 655.0 | 0.28 |
| cli | 1MiB/whole | 16 | 746.9 (742.1–778.1) | 3,004,768.0 | 990.5 | 1,403.96 |
| cli | 8MiB/whole | 128 | 3,574.3 (3,511.6–3,997.1) | 19,134,307.0 | 3,295.0 | 2,346.95 |
| cli | 1MiB/256B | 4,096 | 8,089.9 (8,032.3–8,532.4) | 7,072,327.0 | 79,927.0 | 129.62 |

Frame counts were observed independently over the real transport; every Wire
case also sent one metadata and one terminal frame. An uneven 65,549-byte payload
with 1,009-byte callbacks produced 65 data frames with exact content preservation.
CLI frame counts are inferred from the identical callback sizes and unchanged
Wire framing; its writer tests validate delivered bytes rather than raw frames.

The 64 B case is a proxy for fixed per-result cost, including one data frame,
not a zero-payload intercept. At fixed 1 MiB, whole-payload and 64 KiB callbacks
produce the same 16 frames and have similar allocation counts. Fragmentation to
256 B raises allocations from 987 to 80,002 and median latency from 0.783 to
8.150 ms. No higher frame cap alone can combine those small callbacks.

The empirical difference is approximately **19.37 additional allocations per
additional data frame** from whole to 256 B callbacks, versus **14.64** from
whole to 4 KiB. These are workload-specific slopes, assuming common fixed setup
and equal delivered bytes; they include transport behavior and are not universal
per-frame constants. The five server allocations isolated below are a stronger
source-level attribution than either whole-operation slope.

B/op is non-monotonic: 4 KiB callbacks allocate about 2.44 MB, versus 3.03 MB
for whole-payload callbacks, despite taking longer. The 8 MiB case spans
19.02–20.47 MB and 3,201–3,539 allocations across samples. Pool replenishment,
HTTP/2 framing, GC and scheduling prevent a single fixed-plus-frames-plus-bytes
formula from explaining all totals. Raw samples preserve the full allocation
spread. Small timing differences are inconclusive without a controlled comparison.

Wire and CLI runs use different fixtures and transitive module graphs (notably
x/net); they are separate baselines. Their sequential timings do not isolate the
CLI writer’s incremental overhead. The writer’s own allocations are identified
from its profile instead.

## Allocation attribution and ownership

Profiles use `-test.memprofilerate=1`, 100 iterations for 64 B and 1 MiB whole,
and 10 iterations for 1 MiB/256 B. They ran separately after ordinary timing.
Their instrumented latency is deliberately excluded above. Profiles include
process/setup/teardown allocation: for example, the fragmented profiles contain
one 1 MiB source-payload allocation, outside the timed loop. Client/server setup,
protobuf descriptor initialization, and the CLI fixture's engine construction
also appear. Do not divide entire profile totals by iterations and call the
result benchmark B/op. Default startup sampling before test flag processing and
Go's tiny-allocation packing also limit object-level precision.

The [saved profile text](benchmarks/2026-10-29-streaming/allocation-profiles.txt)
contains flat/cumulative columns and source annotations. Flat cost is allocated
at a site; cumulative cost includes descendants. The rows below overlap through
call stacks and **must not be summed as independent layer costs**.

### Fixed execution and output setup

For the Wire 64 B/100-execution profile (65,875 total sampled objects):

| Site | Flat objects | Cumulative objects | Interpretation |
| --- | ---: | ---: | --- |
| Fixture output construction | 100 | 100 | One 32-byte hosted handle per result; no call history |
| `core.newExecution` | 400 | 1,000 | Execution/channels, plus contexts, ID and event state |
| `newExecutionDelivery` | 300 | 700 | Sender state/channels plus cancellation context |
| Client `openOutput` | 500 | 7,025 | Request/output state, output RPC and header reception |
| gRPC `newClientStream` | 1,812 | 11,888 | Repeated Run/output/release RPC stream setup |
| HTTP/2 `readMetaFrame` | 3,189 | 5,818 | RPC headers and decoding |

Context values, cancellation links, timers, HTTP/2 headers and stream setup dominate
many small-result allocation sites. A single redundant data wrapper has little
leverage over the 652 allocations in the ordinary 64 B result. The report does
not propose changing the established lifecycle to remove those costs.

### Per-data-frame construction and decoding

The Wire fragmented profile delivers 40,960 data frames over ten executions:

| Site | Flat objects | Cumulative objects | Data-frame interpretation |
| --- | ---: | ---: | --- |
| `outputstream.Chunks` | 81,920 | 204,801 | Frame plus initial borrowed-byte wrapper: two/frame |
| `outputWriter.consume.func1` | 81,920 | 122,881 | Detached bytes plus replacement wrapper: two/frame |
| `executionDelivery.send` | 40,981 | 41,118 | One response/frame; includes 20 header/terminal responses and one select allocation |
| Generated output-stream `Recv` | 40,980 | 246,235 | One response per received protocol frame, plus decoding beneath it |
| Protobuf `consumeBytes` | 40,961 | 40,961 | Received payload storage, approximately one/data frame |
| `reflect.unsafe_New` | 82,073 | 82,073 | Primarily decoded nested frame/oneof objects in this workload |
| gRPC protobuf `Marshal` | 82,068 | 124,364 | Serialization buffer wrappers and nested marshaling |
| gRPC `readDataFrame` | 82,061 | 82,061 | HTTP/2 receive storage/wrappers; HTTP/2 frames are distinct from Wire data frames |

Source annotations directly confirm the replaced wrapper: `Chunks` creates an
`OutputFrame_Chunk`, then `consume` assigns another after copying its bytes.
The five server allocations per data frame account for roughly 25.5% of the
fragmented profile's objects. They include the required payload detachment;
only the object-construction portion is a Task 2 opportunity.

### Payload bytes, transport pools, and CLI consumption

In the Wire whole-1-MiB/100-execution profile, payload detachment accounts for
100 MiB plus 37.5 KiB of wrappers, and protobuf `consumeBytes` accounts for
approximately 100 MiB. These are two complete payload-sized allocation paths.
gRPC's sized-buffer pool contributes another 126.0 MiB flat (37.2% of the
profile), with 5.3 MiB in its simple pool. Pool allocation depends on reuse and
scheduling: the equivalent CLI profile has 107.3 MiB in its sized-buffer pool,
while the two payload copies remain approximately 100 MiB each.

The pinned gRPC v1.84.0 source explains relevant discontinuities:
`encoding/proto/proto.go` marshals messages at or below 1 KiB without pooling,
and `internal/transport/http_util.go` similarly chooses receive storage based on
that threshold. `mem/buffer_pool.go` configures default tiers of 256 B, 4 KiB,
16 KiB, 32 KiB and 1 MiB. A serialized 64 KiB chunk therefore requests the
**1 MiB tier**, not a 64 KiB buffer. This is evidence for envelope-aware sizing
experiments; it does not prove that changing the cap will improve end-to-end
performance. The much smaller frames trade buffer behavior against more objects,
serialization and synchronization.

With 256 B callbacks, the Wire byte profile instead attributes 12.19 MiB to
HTTP/2 `readDataFrame`, 11.25 MiB to protobuf marshaling, 10.94 MiB to server
copy-plus-wrapper, and 10.00 MiB to protobuf payload decoding over ten results.
Small frames introduce substantial per-frame storage beyond the two payload
copies. Payload content and length were unchanged throughout.

The CLI writer has 212 flat profiled objects and 6.44 KiB over 100 small results;
its fragmented run has 21 and 656 bytes over ten results. Tiny variables share
allocator blocks, so these profile counts are not an exact count of logical Go
allocations. Their scaling is per-result, not per-frame or payload byte.
`Write`'s large cumulative cost contains client consumption and decoding; it is
not additional CLI copying. No accumulating writer allocation appears.

### Ownership constraints

Runtime callback slices are borrowed. `executionDelivery.send` returns on channel
handoff, while a different goroutine still owns Send; serialization need not have
finished when the hosted callback returns. The server's detached payload copy is
required by this asynchronous contract. An immutable benchmark payload does not
make removing the production copy safe.

The gRPC `ServerStream.SendMsg` contract prohibits modifying a sent message after
SendMsg returns because tracing/stats handlers may observe it lazily. A combined
allocation can share one lifetime among a message's components, but returning
messages or their backing bytes to a pool immediately after Send is unsafe.
Cancellation must still return independently of blocked sends and hosted cleanup;
the execution owner retains cleanup until it settles.

Client Consume borrows decoded chunks synchronously and does not retain the
complete result. Collect deliberately accumulates detached content and would
have a different allocation baseline. Removing protobuf's decoded payload storage
or gRPC serialization buffers requires a broader serialization/buffer-lifetime
contract, not just deleting Wire's wrappers.

## Slow-consumer diagnostic

The separate 8 MiB/64 KiB-callback run stalls the first public consumer callback
for 100 ms. First payload became available 468.333 µs after Run started in the
resume case and 424.584 µs in the cancellation case. These are instrumented
single-run observations, not latency percentiles.

| Observation, same counts in both cases | Producer entered | Producer returned successfully | Send entered | Send returned successfully |
| --- | ---: | ---: | ---: | ---: |
| First callback stalled | 6 | 5 | 5 | 4 |
| After 100 ms | 8 | 7 | 7 | 6 |

Only the first 64 KiB client callback was active; it had not returned. At the
100 ms snapshot, the eighth producer callback and seventh Send were pending.
The production and send counters are independent atomic observations, not a
transactional snapshot. They demonstrate limited progress ahead of the consumer
in this run, without asserting a portable transport queue capacity.

The hosted fixture retains its prebuilt 8 MiB source. Wire can hold one detached
message in Send and one with its producer (up to two 64 KiB payload chunks under
this policy). gRPC/HTTP2/socket storage and the client's decoded callback chunk
are separate. Completed Send calls are not evidence that the client consumed
those bytes. Neither these counters nor benchmark B/op measure peak live memory.

Resuming delivered and validated all 8 MiB; cancellation while stalled propagated
context cancellation. Both paths settled exactly-once hosted cleanup. Existing
`TestExecutionOutputBlockedSendRetainsCleanupOwner` independently verifies the
Wire handoff bound and cancellation/release ownership for request cancellation,
execution cancellation, disconnect and shutdown. The new diagnostic does not
replace that stronger owning-layer contract test.

## Recommendations

1. **Task 2: eliminate the replaced borrowed-byte wrapper first.** The profile
   proves one redundant object per data frame. The potential is about 4,096
   fewer allocations (5.1% of this fragmented operation) and 96 KiB less wrapper
   storage per 1 MiB/256 B execution. With 16 frames the object-count ceiling is
   only about 1.6%; with one frame it is about 0.15%. These are ceilings inferred
   from allocation sites, not achieved improvements or latency predictions.
2. **Evaluate grouped response/frame/final-wrapper allocation.** After eliminating
   the replaced wrapper, those three objects could potentially become one owned
   allocation, saving two more objects/frame: about 10.2% of this fragmented
   operation's allocations. Measure alignment/size-class and escape effects;
   grouped allocation can increase bytes even when object count improves. Keep
   immutable per-message ownership and do not introduce post-Send pooling.
3. **Task 3: test frame sizes with whole 1 MiB/8 MiB producers, a 64 KiB producer,
   and the 64 B fixed-cost guard.** Include envelope-aware candidates below the
   16 KiB/32 KiB pool boundaries alongside the current 64 KiB and a larger cap.
   Keep 4 KiB/256 B producers as controls. Measure throughput, counts, allocated
   bytes and slow-consumer behavior together: the 4 KiB case already shows lower
   allocated bytes but higher latency. Changing a split-only cap cannot improve
   the 256 B producer's frame count.
4. **A separate coalescing task needs representative producer evidence.** The
   synthetic 256 B case establishes a strong potential problem (4,096 frames,
   ~80,000 allocations and ~8.15 ms for 1 MiB), but not its real-world frequency.
   Capture callback-size distributions from the native CLI workload after its
   dependencies build. Frequent small callbacks plus acceptable added first-byte
   latency and a bounded accumulation/flush contract would justify that task.
5. **Leave payload-copy elimination and small-result lifecycle redesign outside
   Tasks 2/3.** The two payload-sized paths and transport pool behavior dominate
   large byte totals. Removing their allocations requires broader hosted-output,
   decoder or transport ownership changes. The CLI shared writer's own fixed
   allocation is small and provides no evidence for per-frame writer optimization.

## Validation and limits

Passed on Go 1.27.3 unless noted:

```sh
make fmt
make check-fmt
make test
make build
go test ./test/integration -run '^TestOutputDelivery' -count=1 -v
go test -race ./test/integration ./server/internal/grpcserver
go test -mod=readonly ./test/integration ./server/internal/grpcserver \
  -run '^(TestOutputDelivery|TestExecutionOutputBlockedSendRetainsCleanupOwner|TestExecutionOutputWaitsForTerminalSend)' -count=1
go test -mod=readonly -race ./test/integration ./server/internal/grpcserver
GOTOOLCHAIN=go1.26.5 make lint
```

CLI validation (from its repository):

```sh
GOWORK=off go test -mod=readonly ./internal/output ./internal/testutil/wirehost
GOWORK=off go test -mod=readonly -race ./internal/output ./internal/testutil/wirehost
GOWORK=off go test -mod=readonly -modfile=/tmp/wire-streaming-baseline-20261029/frozen/cli.mod ./internal/output ./internal/testutil/wirehost
GOWORK=off go test -mod=readonly -modfile=/tmp/wire-streaming-baseline-20261029/frozen/cli.mod -race ./internal/output ./internal/testutil/wirehost
GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.26.5 staticcheck ./internal/output ./internal/testutil/wirehost
revive -config revive.toml internal/output/wire_delivery_benchmark_test.go internal/output/wire_delivery_fixture_test.go internal/output/wire_delivery_writer_test.go
```

The three new CLI files were formatted with gofmt and
`goimports -w -local github.com/MontFerret`; broad CLI formatting was avoided to
preserve its existing migration. Both frozen benchmark families completed all ten
ordinary samples, all six high-resolution profiles completed, and the frozen
Wire diagnostic tests passed. Fixture tests validate content, uneven boundaries,
terminal/cleanup errors, exactly-once cleanup, stalled cancellation and repeated
CLI executions. Existing CLI writer tests cover writer failures and short writes.

The pinned golangci-lint cannot decode Go 1.27 export data (version 5 versus its
maximum 4). `make lint` failed for that tool incompatibility; the same pinned
linter passed for both root and native modules with the installed Go 1.26.5
loader. Initial spacing findings in the new Wire tests were corrected and lint
rerun successfully. No performance sample uses the older toolchain.

The CLI runtime benchmark remains unavailable: LLM rc.6 and HTML rc.23 dependencies
call the old Ferret Encode/Decode signatures. The exact build failure is saved in
[cli-runtime-unavailable.txt](benchmarks/2026-10-29-streaming/cli-runtime-unavailable.txt).
Its vendor directory also predates the migration. Neither dependency repair nor
vendor regeneration is part of this baseline. Full CLI/native-query performance,
process startup, historical CLI percentages, remote-network performance, and peak
live memory are unvalidated. No production/protobuf changes required generation
or breaking checks; full native Ferret execution/race suites were not rerun.

The final self-review covered the complete task diff, benchmark lifecycle and
fairness, fixture retention, diagnostic overhead, concurrency, module selection,
source provenance and every performance claim. It corrected a frame-count table
transcription, checked the report matrix against the observed cases, and added
artifact-directory and frozen-binary integrity checks to the measurement script.
The finalized script's diagnostic mode passed in a separate temporary directory;
the benchmark sources still match the frozen source hashes. All 57 pre-existing
CLI migration paths match their pre-task hashes. Results describe this machine and
these fixtures; no optimization has been implemented or measured against them yet.
