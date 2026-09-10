---
title: Q9 RowData release comparison
description: Partitioned Top-1 admission, bounded release measurements, and mixed JVM/native profiles.
---

Q9 now accelerates through ordinary whole-plan selection on in-memory and default RocksDB state.
Its auction/bid range join feeds append-only partitioned Top-1, ordered by bid price descending
and bid timestamp ascending. Flink retains planning, routing, resource assignment and recovery.

At 250,000 events, the combined six-fork median input throughput is 23.4% below Flink in memory
and 3.11 times Flink on RocksDB. The RocksDB advantage does not repeat across measurement sets:
the first three-pair set gives 3.62×, the second 0.95×. Both sets and every fork are retained below.
These short end-to-end runs show substantial variation and establish no reliable general or
steady-state speedup. The September 8 one-million-event in-memory attempt fails on state capacity;
the later shared-memory capacity check below completes that size.
Separate 500,000-event CPU profiles complete on both backends; their timings are excluded below.

The September 10 admission recheck passes eight `NexmarkQ9ProductionIT` cases explicitly covering
Flink's default disabled multi-join optimizer and the enabled preset, both backends and
parallelism one/four. Each case compares final collected bytes at 10,000 events and separately
checks positive unmodified-blackhole output and native activity. Shared-plan execution retains
zero standalone Top-N invocations. These are integration checks, not new performance results;
the measurements and capacity failures below remain tied to their September 8 release.

## September 10 shared-memory capacity check

Release `ee008d54c3de84a9e83278e9d9fa73998ad66426` replaces private native-operator ceilings
with the slot's shared OPERATOR pool. Q9 now completes one million events through the unmodified
blackhole sink on both backends. The immediately preceding `f5d707c9` in-memory attempt failed
state-mutation admission, requesting 694,848 bytes with 522,756 available. Both releases use
the default disabled multi-join optimizer, parallelism four, UTC, disabled mini-batching,
one-second exactly-once checkpoints, a 1 GiB JVM heap, 1 GiB managed memory, 2 GiB maximum JVM
direct memory and weights `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`. No budget was increased.

Each new blackhole capacity case has one fresh JVM per engine. These are completion checks,
not a throughput comparison; their times and single-fork ratios are not benchmark results.
StreamFusion emits 159,713 changelog records in each case, with native-plan / Calc counts
2,282/330 in memory and 2,230/322 on RocksDB. Flink emits 159,751 and 159,518 respectively;
intermediate winner transitions depend on input interleaving. All native jobs report acceleration.

The first million-event in-memory collecting pair completes with 59,959 final rows each, but
its strict final-output hash comparison fails. Three diagnostic reruns (Flink, StreamFusion,
Flink) establish that two unmodified Flink jobs themselves choose different final payloads:
only auction `23839` differs, at price `880617` and bid timestamp `2020-09-13T12:26:40`.
All ordering fields and the other 59,958 rows agree. The tied bids' extra strings have lengths
64 and 74; Q9 supplies no further tie-breaker. StreamFusion matches the first diagnostic Flink
result completely. This is not evidence that every independently scheduled final result set
must hash identically, and the original failed comparison is retained.

The two observed in-memory final hashes are
`06b9b0afc81bff8511412175323867b896e2d3085b4262d45eb2a4014fef4ad6` and
`7df0c691a1b09657a9ee0f3eade18d04af4293b06e97403eb52431906db364f5`.
Fixed-arrival operator tests compare every transition, including exact ordering-key ties with
distinct payloads. The query and generator are unchanged; no additional tie-breaker or relaxed
production comparator is introduced. This check establishes removal of the demonstrated memory
admission failure, not lower physical memory consumption or completion of Q9 performance work.

The separate million-event RocksDB collecting pair matches all 59,959 materialized rows and
the `06b9b0af...` hash above exactly. Eight focused Top-1 conformance cases also pass, comparing
the complete byte-level changelog for identical arrival order across both backends and multiple
batch sizes, including distinct payloads tied on every ordering field. The shared-memory change
passes 121 other selected memory, join parity, metric, original-buffer-geometry, recovery and
Q5/Q8/Q9 production-integration checks.

Evidence remains under `streamfusion-nexmark-benchmarks/target/measurements/q9-default/ee008d54/`,
including blackhole metadata/logs, the original `collecting-validation-1000000.json`, diagnostic
row dumps, `collecting-rocksdb-validation-1000000.json` and `winning-bid-differences.json`.
The previous `f5d707c9/` failure remains separate.

## Measurements, September 8, 2026

Each backend uses six fresh unprofiled JVMs per engine, in two sets of three pairs. Engine order
alternates continuously: Flink/StreamFusion, StreamFusion/Flink, repeated three times. The second
set was added because a separate wall-clock diagnostic did not reproduce the relative elapsed-time
ordering. It does not replace the first set. MAD is median absolute deviation; throughput counts
250,000 generated input events per fork. Combined medians pool all six timings per engine.

| Set | Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | --- | --- | --- | ---: |
| All six pairs | In-memory | 5.249 [5.111–8.183]; 0.124 | 6.852 [5.708–12.093]; 1.110 | 0.766× |
| First three pairs | In-memory | 7.332 [5.264–8.183]; 0.851 | 8.413 [5.923–12.093]; 2.489 | 0.872× |
| Next three pairs | In-memory | 5.138 [5.111–5.234]; 0.027 | 5.776 [5.708–7.781]; 0.068 | 0.890× |
| All six pairs | RocksDB | 20.716 [6.084–31.558]; 5.907 | 6.667 [5.959–11.399]; 0.692 | 3.107× |
| First three pairs | RocksDB | 21.689 [20.305–31.558]; 1.384 | 5.992 [5.959–7.221]; 0.033 | 3.620× |
| Next three pairs | RocksDB | 9.259 [6.084–21.128]; 3.175 | 9.743 [6.113–11.399]; 1.656 | 0.950× |

Every StreamFusion fork reports ordinary acceleration and positive native plan activity; Flink
records zero native invocations. Plan/Calc counts are 697–730/132–138 in memory and
802–921/152–180 on RocksDB. These batch diagnostics are separate from logical Flink I/O metrics.

## Method and correctness

The clean measured checkout is `3d0913b420ea9c74fb5c4d9df0e3a481a966f48e`. Both engines use the
same bounded official Nexmark RowData source, Q9 SQL and unmodified Flink blackhole sink.
End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup, execution and
cleanup; it excludes JVM launch and argument parsing. These are not steady-state rates.

The host is WSL2 Linux, Intel Core i7-12650H, 16 reported logical CPUs, approximately 7.6 GiB RAM
and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, a fixed 1 GiB heap, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Flink settings are parallelism
four, 1 GiB managed memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. The existing
multi-join optimizer setting is identical. Compilation does not overlap measurements.

Rust 1.94 uses release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`.
Separate DWARF files retain release optimization; packaged CPU metadata is verified at load.
Artifact SHA-256 values:

- Core: `e60cc53fce388d85c847bb886016842a4c220352b0035c9a86c66d6aea2d205e`.
- RocksDB: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

`NexmarkQ9ProductionIT` separately checks final keyed result bytes at 10,000 events, parallelism
one and four, on both backends. It requires ordinary selection, positive shared native plan activity
and zero standalone Top-N invocations. Generated Top-1 SQL compares complete changelog bytes.
Identical-arrival operator tests compare every transition and the full Flink metric/control surface,
including nullable keys/order values, tied keys with distinct payloads and timestamp endpoints.
Recovery tests cover canonical backend switching, one-to-two-to-one rescaling, incremental RocksDB
snapshots, and actual aligned/unaligned Arrow channel replay through the shared native plan.

Independent Q9 jobs have a scheduling-dependent intermediate changelog, including within
unmodified Flink: two repeated Flink runs emitted 1,537 and 1,495 changelog records but ended with
the same 593 rows. All sixteen scheduling-diagnostic runs matched final bytes within their
backend/parallelism configuration. This report does not claim identical transient changelogs
between independently scheduled jobs. The identical-arrival tests retain that strict comparison.
Blackhole timing itself provides no output-parity evidence.

## General implementation changes

Top-1 uses DataFusion sorting and cumulative MIN over fixed-width priority ordinals. It preserves
Flink's per-arrival INSERT/UPDATE_BEFORE/UPDATE_AFTER transitions and strict arrival tie-breaking.
Using ordinals avoids repeating a wide winner string for every candidate. Its shared Calc → Top-1 →
Calc path retains native Arrow ownership and stage metrics. It batches point-state reads, writes
only changed winners, and migrates older ordered Top-1 state. Losing arrivals cause no state write.
These are general operator changes, with no Q9-specific SQL rewrite or source special case.

Wide join predicates now use a row and byte bound and halve candidate chunks on reservation denial,
before allocating their Arrow arrays. The full per-pair allowance remains unchanged. Under pressure,
the join emits an already-admitted output prefix and resumes its cursor; an empty output can reduce
fan-out to the minimum two slots needed for an outer-join transition. State access still occurs at
input-batch boundaries. Tests cover hot keys, all join transition families, exact state, budget denial
and credit release. These changes reduce temporary materialization without changing Flink's budget.

## Capacity limit and failed larger attempts

The one-million-event in-memory run still fails at the measured commit. Dirty join-state encoding
requests another 262,416 bytes with 7,700,204 bytes already reserved by its in-flight-state consumer
and 139,125 bytes available. The successful Flink fork took 20.939743 seconds and is diagnostic only.
There is no completed million-event comparison and no million-event RocksDB result.

Earlier million-event attempts are retained as well:

- `524cf40f`: a predicate workspace requested 17,571,236 bytes with 16,059,870 available.
- `da5969c9`: the byte-bounded predicate chunk requested 8,386,348 bytes with 4,856,345 available.
- `1e44fed9`: output growth requested another 4,796 bytes with 4,643,496 already reserved and 3,219
  available. Its successful Flink fork took 36.006814 seconds and is diagnostic only.

The smaller comparison above is bounded evidence. It does not establish sustained operation at
larger retained-state sizes. State allowances, consumer weights and query semantics were preserved.

## Mixed JVM/native profiles

Separate 500,000-event forks capture async-profiler 4.5 CPU samples every 10 ms with Java
non-safepoint sampling and native DWARF unwinding, plus sampled Java allocations at a 2 MiB interval.
JFR, CPU collapsed stacks/flame graphs, allocation collapsed stacks and differential flame graphs
are retained. Percentages are inclusive shares of all process CPU samples and overlap. JNI includes
downstream execution, and DataFusion includes custom children beneath its execution framework.
Zero samples do not imply zero cost. Totals are 2,688/2,409 samples for Flink/StreamFusion in memory
and 2,985/2,713 on RocksDB.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Source polling and descendants | 17.746% | 12.246% | 10.620% | 12.532% |
| RowData copying | 11.310% | 3.902% | 7.303% | 3.760% |
| RowData-to-Arrow writing | 0.000% | 1.370% | 0.000% | 1.032% |
| Arrow C Data/JNI | 0.000% | 10.004% | 0.000% | 12.901% |
| Native plan lowering | 0.000% | 0.000% | 0.000% | 0.037% |
| DataFusion execution | 0.000% | 5.479% | 0.000% | 5.898% |
| Native Top-1 | 0.000% | 1.660% | 0.000% | 2.064% |
| Top-1 DataFusion selection | 0.000% | 0.623% | 0.000% | 0.700% |
| Top-1 state encoding | 0.000% | 0.042% | 0.000% | 0.111% |
| Native join | 0.000% | 3.113% | 0.000% | 4.165% |
| Memory-budget callbacks | 0.000% | 0.706% | 0.000% | 0.184% |
| Arrow-backed row access | 0.000% | 0.955% | 0.000% | 0.774% |
| RocksDB and native plugin boundary | 0.000% | 0.000% | 17.219% | 5.529% |
| Garbage collection | 4.092% | 4.525% | 3.216% | 2.138% |
| Native artifact loading | 0.000% | 1.577% | 0.000% | 1.622% |
| JVM compilation | 36.235% | 40.681% | 36.248% | 40.288% |

The profiles show lower sampled row-copy and RocksDB CPU work in StreamFusion. Flink's RocksDB
profile includes repeated MapState iteration, reads and writes; StreamFusion batches state access.
Top-1 DataFusion selection is under 1% of process CPU samples and native Top-1 is about 2%.
JVM compilation is 36–41%, limiting conclusions about sustained operator throughput. CPU sampling
alone does not account for the full elapsed-time gap on RocksDB.

Java allocation sample counts are 3,890/2,326 in memory and 4,480/2,231 on RocksDB for
Flink/StreamFusion, dominated by byte arrays, memory segments and row/string objects. These are
sample counts, not allocated bytes, retained memory or a native allocator profile. Verified native
symbols expose the execution and state call chains, though some samples end in an unresolved libc
leaf and cannot be attributed to a specific allocation/copy function. The profiled StreamFusion
runs report 1,276/232 plan/Calc invocations in memory and 1,580/296 on RocksDB.

A separate 500,000-event RocksDB wall-clock diagnostic samples all threads every 10 ms with
thread identities and native unwinding. It contains 4,364/9,387 Flink task-stack wall samples for
Flink/StreamFusion, including 2,231/7,738 in unavailable-input mailbox waiting, 622/139 in synchronous
checkpoint work and 49/106 at `fsync`/`fdatasync`. These categories overlap and aggregate threads;
they are not process elapsed seconds. The instrumented fork reversed the earlier elapsed-time
ordering, motivating the second unprofiled set. Neither its timings nor CPU-profile timings enter
the measurement table. The diagnostics expose checkpoint/filesystem and scheduling variation,
but do not identify a stable cause for the entire first-set RocksDB gap.

This evidence does not justify specializing the query or replacing the DataFusion computation.
Further sustained-throughput work first needs to address the larger join-state capacity limit;
this checkpoint retains that limitation explicitly.

## Reproduction and retained evidence

Build/install release artifacts first, then use `NexmarkBlackholeBenchmark` as described in the
[RowData blackhole method](/StreamFusion/benchmarks/rowdata-blackhole/), with `q9`, parallelism four,
and the settings above. Run each engine in its own JVM and alternate the order for at least three
unprofiled pairs. Run longer profiled forks separately.

Local evidence is under `streamfusion-nexmark-benchmarks/target/measurements/q9/3d0913b4/`, divided
by `hashmap`/`rocksdb`, `250000`, `250000-continuation` and `profiles-500000`. The extra RocksDB
wall-clock diagnostic is under `rocksdb/wall-500000`. The failed million-event attempt is under
`hashmap/1000000`; earlier failed runs are under their respective commit directories. Evidence
includes exact commands, native build metadata, machine/runtime details, EXPLAIN/logs, every fork,
medians/ranges/MAD and profile artifacts. Generated profiles are not checked in.
