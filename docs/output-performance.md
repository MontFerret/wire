# Alpha.21 output measurements

Measured locally on an Apple M2 Max, darwin/arm64, Go 1.27.3, with the default
GOMAXPROCS of 12. Figures below are medians of three runs. These are in-process
gRPC benchmarks, not network service-level guarantees.

## Existing execution benchmarks

The same command ran before migration and after implementation:

```sh
go test -mod=mod ./server ./server/internal/core -run '^$' \
  -bench 'Benchmark(RuntimeAdapterDurableSession|RunDurableSession|ExecutionEventPublication)$' \
  -benchmem -count=3
```

| Benchmark | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Public durable session | 81,101 | 92,325 | 35,595 | 40,431 | 568 | 661 |
| Core durable session | 4,542 | 6,050 | 1,648 | 3,233 | 34 | 60 |
| Event publication, no watchers | 23.76 | 22.95 | 0 | 0 | 0 | 0 |
| Event publication, one watcher | 63.49 | 58.99 | 0 | 0 | 0 | 0 |

The public small-result path costs about 14% more time, 14% more allocated bytes,
and 16% more allocations. The core path now includes an output consumption handoff
and finalization; its fixture also collects bytes separately from lifecycle history.
This replaces the previous single buffered terminal snapshot. No buffered fast path
is retained. The event publication path remains allocation-free.

The regression was investigated with CPU and allocation profiles. The CPU sample
was dominated by scheduler/condition-variable waits and signals; allocations were
distributed through gRPC stream/header processing, context cancellation links,
protobuf messages, and output ownership. A second three-second profiled public run
measured 92,642 ns/op, 40,512 B/op, and 661 allocs/op. The initial combined benchmark
run, immediately after large debugger replays, measured approximately 131 us/op;
the isolated baseline command above is the comparison used here. These results do
not establish a single encoding hotspot. Chunk sizing avoids binary-search
allocations when a full 64 KiB chunk fits the message budget.

## New output benchmarks

```sh
go test ./server ./server/internal/core ./test/integration -run '^$' \
  -bench 'Benchmark(RuntimeAdapterDurableSession|RunDurableSession|ExecutionEventPublication|ExecutionOutput|DebuggerOutputReplay)$' -benchmem -count=3
```

| Benchmark | Payload | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Execution Consume | 64 B | 109,680 | 44,418 | 693 |
| Execution Consume | 8 MiB | 3,279,238 | 18,954,042 | 4,045 |
| Debugger terminal replay | 64 B | 41,700 | 16,861 | 282 |
| Debugger terminal replay | 8 MiB | 4,217,238 | 34,614,255 | 3,602 |

Execution benchmarks discard borrowed chunks after each callback. Debugger replay
benchmarks validate raw framed content, so they include server snapshot copying and
transport delivery but exclude a public client's materialized accumulator. The
public debugger contract does require materialization. B/op is cumulative allocation,
not peak or retained memory, and large-run allocation results vary with gRPC pooling.

## Bounded buffering and cancellation

`TestBlockedOutputSendDoesNotHoldProducerCleanup` stalls the actual delivery
callback, permits at most two detached messages at the producer/sender handoff,
and verifies producer cleanup and handler return without unblocking the send.
`TestUnreadOutputRetainsRuntimeUntilAbandonment` verifies that abandonment does not
drain an arbitrarily large producer. `TestUnreadOutputFinalizesAfterLifetimeEnds`
covers cancellation, disconnect, and shutdown during unread delivery.

These checks establish Wire's bounded normal-output handoff, independently of
benchmark allocation totals. gRPC has its own flow-control and transport buffers.
Hosted runtimes may materialize their output; Collect and debugger content
deliberately materialize delivered bytes. Debugger terminal content remains retained
for replay until release. There is no aggregate output-size limit.
