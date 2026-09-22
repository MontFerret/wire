# Alpha.19 migration benchmarks

Measured on darwin/arm64, Apple M2 Max, Go 1.26.5. Each result is the median of
three runs. Table entries are **ns/op / B/op / allocs/op**. These are local
measurements, not a performance guarantee.

The initial baseline was captured before editing. Because wall-time results
shifted during the session, the unchanged HEAD was also extracted with
`git archive HEAD` into a temporary directory and rerun immediately before the
final implementation. Benchmarks ran sequentially without concurrent test/lint
jobs. Both baselines are retained to expose the measurement variation.

| Benchmark | Initial alpha.11 baseline | Rechecked alpha.11 baseline | Alpha.19 |
| --- | --- | --- | --- |
| `RuntimeAdapterDurableSession` | 81576 / 35470 / 586 | 121280 / 35338 / 582 | 110979 / 35504 / 586 |
| `ExecutionEventPublication/no_watchers` | 26.2 / 0 / 0 | 29.44 / 0 / 0 | 28.24 / 0 / 0 |
| `ExecutionEventPublication/one_watcher` | 68.2 / 0 / 0 | 72.89 / 0 / 0 | 71.95 / 0 / 0 |
| `CancelExecution` | 66.83 / 48 / 1 | 75.16 / 48 / 1 | 74.4 / 48 / 1 |
| `RunDurableSession` | 3976 / 1600 / 32 | 4534 / 1599 / 32 | 5022 / 1647 / 33 |
| `ReplaceBreakpoints` | — | — | 18603 / 32840 / 28 |

Publication and cancellation retain their allocation counts. The core durable
session run adds one 48-byte allocation for the hosted pointer output; its
median time was about 11% above the rechecked baseline. The public gRPC durable
session benchmark showed no comparable slowdown against the rechecked baseline,
but wall-time variation precludes a speedup claim. Its allocations rose by four
against the recheck and matched the original baseline's 586 allocations.

`ReplaceBreakpoints` measures 32 stable requests replaced atomically through the
core owner and a hosted API spy. It includes hosted enumeration, ID matching,
quota accounting, and detached result copies. It has no pre-migration equivalent.

Commands:

```sh
go test ./server ./server/internal/core -run '^$' -bench 'Benchmark(RuntimeAdapterDurableSession|RunDurableSession|ExecutionEventPublication|CancelExecution)$' -benchmem -count=3
go test ./server ./server/internal/core -run '^$' -bench 'Benchmark(RuntimeAdapterDurableSession|RunDurableSession|ExecutionEventPublication|CancelExecution|ReplaceBreakpoints)$' -benchmem -count=3
```

All runs used the same task-scoped writable GOCACHE. Raw successful measurements
follow; the second baseline is the unchanged alpha.11 source, not a dependency-only
downgrade of the implementation.

## Initial baseline

```text
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server
cpu: Apple M2 Max
BenchmarkRuntimeAdapterDurableSession-12    	   14534	     82457 ns/op	   35547 B/op	     586 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	   14810	     80968 ns/op	   35470 B/op	     586 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	   14703	     81576 ns/op	   35402 B/op	     586 allocs/op
PASS
ok  	github.com/MontFerret/wire/server	3.936s
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server/internal/core
cpu: Apple M2 Max
BenchmarkExecutionEventPublication/no_watchers-12         	46251240	        26.20 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	45794534	        26.21 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	46869201	        25.57 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	17996523	        67.17 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	17610703	        68.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	17194922	        68.20 ns/op	       0 B/op	       0 allocs/op
BenchmarkCancelExecution-12                               	17717167	        66.45 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	18310522	        66.83 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	17669979	        67.08 ns/op	      48 B/op	       1 allocs/op
BenchmarkRunDurableSession-12                             	  304342	      3914 ns/op	    1600 B/op	      33 allocs/op
BenchmarkRunDurableSession-12                             	  301864	      3976 ns/op	    1600 B/op	      32 allocs/op
BenchmarkRunDurableSession-12                             	  291579	      4011 ns/op	    1599 B/op	      32 allocs/op
PASS
ok  	github.com/MontFerret/wire/server/internal/core	14.831s
```

## Unchanged baseline recheck

```text
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server
cpu: Apple M2 Max
BenchmarkRuntimeAdapterDurableSession-12    	    9447	    111961 ns/op	   35461 B/op	     583 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	    9964	    123626 ns/op	   35338 B/op	     582 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	    9920	    121280 ns/op	   35329 B/op	     582 allocs/op
PASS
ok  	github.com/MontFerret/wire/server	3.962s
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server/internal/core
cpu: Apple M2 Max
BenchmarkExecutionEventPublication/no_watchers-12         	45160034	        27.77 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	43835816	        29.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	38323455	        29.73 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	16820739	        72.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	16872297	        72.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	14167692	        76.50 ns/op	       0 B/op	       0 allocs/op
BenchmarkCancelExecution-12                               	14922874	        75.89 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	15572692	        73.98 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	16766616	        75.16 ns/op	      48 B/op	       1 allocs/op
BenchmarkRunDurableSession-12                             	  279268	      4534 ns/op	    1600 B/op	      32 allocs/op
BenchmarkRunDurableSession-12                             	  290020	      4504 ns/op	    1599 B/op	      32 allocs/op
BenchmarkRunDurableSession-12                             	  258192	      4642 ns/op	    1599 B/op	      32 allocs/op
PASS
ok  	github.com/MontFerret/wire/server/internal/core	15.276s
```

## Final implementation

```text
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server
cpu: Apple M2 Max
BenchmarkRuntimeAdapterDurableSession-12    	   10657	    110979 ns/op	   35608 B/op	     586 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	    9385	    111587 ns/op	   35504 B/op	     586 allocs/op
BenchmarkRuntimeAdapterDurableSession-12    	   10000	    104229 ns/op	   35464 B/op	     586 allocs/op
PASS
ok  	github.com/MontFerret/wire/server	3.747s
goos: darwin
goarch: arm64
pkg: github.com/MontFerret/wire/server/internal/core
cpu: Apple M2 Max
BenchmarkReplaceBreakpoints-12           	   68432	     18123 ns/op	   32840 B/op	      28 allocs/op
BenchmarkReplaceBreakpoints-12           	   62622	     18603 ns/op	   32840 B/op	      28 allocs/op
BenchmarkReplaceBreakpoints-12           	   67670	     18871 ns/op	   32840 B/op	      28 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	41942336	        28.24 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	44673673	        27.16 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/no_watchers-12         	44341117	        28.38 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	16569283	        72.42 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	15037671	        71.79 ns/op	       0 B/op	       0 allocs/op
BenchmarkExecutionEventPublication/one_watcher-12         	16608086	        71.95 ns/op	       0 B/op	       0 allocs/op
BenchmarkCancelExecution-12                               	15350977	        76.89 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	16943258	        71.88 ns/op	      48 B/op	       1 allocs/op
BenchmarkCancelExecution-12                               	16672628	        74.40 ns/op	      48 B/op	       1 allocs/op
BenchmarkRunDurableSession-12                             	  246132	      4920 ns/op	    1648 B/op	      33 allocs/op
BenchmarkRunDurableSession-12                             	  255172	      5022 ns/op	    1647 B/op	      33 allocs/op
BenchmarkRunDurableSession-12                             	  232797	      5135 ns/op	    1647 B/op	      33 allocs/op
PASS
ok  	github.com/MontFerret/wire/server/internal/core	18.850s
```
