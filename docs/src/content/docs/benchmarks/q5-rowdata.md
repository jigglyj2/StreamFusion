---
title: Q5 RowData release comparison
description: Shared HOP window admission, release measurements, and managed-memory limits.
---

Q5 is admitted on in-memory and default RocksDB state with the benchmark's
`table.optimizer.multi-join.enabled=true` preset. With Flink's default `false`, the September 10
audit instead selects `StreamExecWindowJoin` and retains whole-plan fallback. The measurements
below establish the enabled-preset path only; default window-join integration remains unfinished.
Its local/global
HOP COUNT feeds both a binary join and an attached MAX branch. The reused aggregate has one
native owner; its Calc and attached local MAX consume shared Arrow batches in that same native
plan. Flink retains exchanges, resource assignment, checkpoints and recovery.

At one million input events, StreamFusion has 0.9% lower median input throughput in memory and
10.8% higher median input throughput on RocksDB. A ten-million-event in-memory attempt fails
when Flink denies join-state decoding workspace. This checkpoint does not establish a large-input
capacity or steady-state performance advantage.

## Measurements, September 8, 2026

Each engine ran in three fresh, unprofiled JVMs. Engine order alternated Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation.

| Backend | Input events | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | ---: | --- | --- | ---: |
| In-memory | 1,000,000 | 6.982 [6.810–7.067]; 0.085 | 7.047 [6.801–8.216]; 0.246 | 0.991× |
| RocksDB | 1,000,000 | 7.565 [7.293–9.664]; 0.273 | 6.830 [6.784–6.975]; 0.046 | 1.108× |

Median input rates are 143,225 versus 141,911 events/s in memory and 132,179 versus 146,402
events/s on RocksDB, respectively Flink and StreamFusion. Memory ranges overlap: the small median
difference is approximately a tie, with no demonstrated advantage. The slow StreamFusion memory
fork and slow Flink RocksDB fork are retained. Three pairs on one host do not establish a universal
speedup.

Every StreamFusion fork reports `Accelerated: yes`. Memory forks record 618–625 native plan
invocations and 68 Calc batches; RocksDB forks record 758–783 and 128–132. Flink records zero
native invocations. These diagnostics count native batches/streams; operator I/O metrics count
logical Flink records.

## Method and correctness boundary

The clean benchmark checkout is `5587afa0093d3206631334c79bed66b288e39ac9`. Both engines use the
same deterministic bounded Nexmark RowData source, Q5 SQL and unmodified Flink blackhole sink.
The end-to-end timer includes
table setup, ordinary EXPLAIN preflight, StreamFusion native initialization, local cluster
startup, execution and cleanup. JVM launch and argument parsing are outside it; separate process
wall times are retained. These are not steady-state rates.

The machine is WSL2 Linux on an Intel Core i7-12650H, reporting 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, `-Xms1g -Xmx1g`,
`-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the Arrow-required `java.nio`
opening. Flink settings are parallelism four, 1 GiB managed memory, consumer weights
`OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled mini-batching, one-second exactly-once
filesystem checkpoints and no restart. Both engines use the harness's multi-join optimizer
setting. Measurements run without concurrent compilation.

Native artifacts use Rust 1.94 release optimization, `target-cpu=native`, frame pointers and
`debuginfo=1`; symbols do not reduce optimization. Runtime ELF libraries have separate DWARF
files connected by GNU debug links, and CPU metadata is verified at load time. Packaged SHA-256:

- Core: `9af5a856e1bf3510e76bf39d00fac14c4a9959db979f59c42c0c9571d69fc77d`.
- RocksDB state: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

Opt-in official Nexmark integration compares complete collected changelog bytes and materialized
results at 10,000 events, parallelism one and four, on both backends. It requires ordinary
acceleration, positive native plan activity and zero calls to the retained standalone
local-window JNI path. Generated shared-runtime tests compare exact Flink changelogs, complete
default metric surfaces, original local-window pressure/flush boundaries, canonical backend
switching, rescaling, and aligned/unaligned input-channel replay through both Arrow exits.
These controlled tests supplement independent-job parity. Blackhole timing establishes no
correctness claim. The packaged Flink CLI runner separately verifies shared-window selection,
native execution, exact collected Rows and optional RocksDB artifact loading through the isolated
planner classloader.

## General memory improvements and capacity limit

Larger diagnostic runs exposed reservations that substantially exceeded the required buffers.
The fixes preserve DataFusion computation, Flink flush boundaries and the same deployment budget:

- Local window hash tables store ordinals into one retained Arrow-key vector. They reserve
  replacement index capacity only when growth requires it. DataFusion consumes at most 2,048
  rows at a time through zero-copy slices of the incoming batch; this does not emit extra partials.
- Global slice decoding reserves its logical partial bytes instead of repeatedly charging
  an entire retained parent Arrow allocation for small slices.
- Primitive scalar broadcasts reserve by Arrow physical width and validity, rather than
  multiplying Rust enum overhead by the output row count. DataFusion still materializes values.
- Ordered in-memory state stores immutable boxed key/value bytes and replaces values without
  removing and reinserting existing keys. Canonical persisted bytes remain unchanged.
- Paged join decoding reserves payload bytes and row-vector headroom in one batch admission.
  Original and updated state share payload Arcs; wide rows are not reserved as eight copies.

Constrained-budget regressions cover 100,000 local keys with 1,024- and 16,384-row input batches,
small global-window slices of large parents on both backends, 16,384-row primitive broadcasts,
and 100,000 ordered state entries including deletion and snapshot/restore. Generated Java parity,
metric and recovery tests passed after these changes. No per-query algorithm, allocator hook,
deployment tuning knob or quota bypass was introduced.

At the measured commit, the ten-million-event memory diagnostic completed Flink in 17.240 seconds
but StreamFusion failed: join in-flight state requested 3,337,136 bytes while retaining
48,452,344 bytes, with 3,307,464 bytes available from its shared Flink allowance. Other consumers
also use that allowance, so the join workspace figure is not the complete region footprint.
The join still decodes historical pages for every touched key at batch admission; very large
keys can exceed the allowance even with bounded output. There is no completed comparison at that
size and no ten-million-event RocksDB measurement. Earlier failed
attempts and their logs are retained. Memory limits were not increased to obtain a passing result.

The earlier one-million-event series at `55507db3` reported ratios of 0.989× in memory and
1.117× on RocksDB. Flink's larger diagnostic times varied substantially across attempts; these
series do not isolate the effect of any individual fix. The final matched comparison above is
the performance result. Constrained-memory tests establish the intended reduction in required
reservations independently of elapsed-time variation.

## Separate CPU and allocation profiles

Each engine/backend pair has a separate longer two-million-event fork recorded with
async-profiler 4.5, 10 ms CPU sampling, Java non-safepoint sampling, native DWARF unwinding,
Java allocation sampling at 2 MiB and JFR output. Profiled timings are excluded from measurements.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Source polling and synchronous downstream path | 20.72% | 18.54% | 13.22% | 14.23% |
| RowData serializer / binary row copying | 10.00% | 4.73% | 5.97% | 3.17% |
| RowData-to-Arrow writer | 0.00% | 1.45% | 0.00% | 1.36% |
| Arrow C Data / JNI execution boundary | 0.00% | 23.59% | 0.00% | 37.94% |
| Native plan lowering | 0.00% | 0.00% | 0.00% | 0.00% |
| DataFusion execution | 0.00% | 22.03% | 0.00% | 18.47% |
| Native local window | 0.00% | 0.65% | 0.00% | 0.40% |
| Native global window | 0.00% | 8.43% | 0.00% | 7.62% |
| Native streaming join | 0.00% | 12.29% | 0.00% | 10.26% |
| JVM memory-budget reserve/release callbacks | 0.00% | 0.33% | 0.00% | 0.29% |
| Arrow-backed row access / sink adapter | 0.00% | 0.04% | 0.00% | 0.00% |
| RocksDB | 0.00% | 0.00% | 43.02% | 20.78% |
| Garbage collection | 4.88% | 3.16% | 2.11% | 2.51% |
| StreamFusion artifact initialization | 0.00% | 1.38% | 0.00% | 1.20% |
| JIT compilation | 32.76% | 33.77% | 23.99% | 27.84% |

Denominators are all process CPU samples: 2,949 / 2,751 for memory and 4,840 / 3,753 for RocksDB
(Flink / StreamFusion). Categories overlap. JNI includes native descendants; DataFusion includes
the execution-plan framework and its descendants, including custom Flink state adaptations.
Source polling includes the synchronous downstream chain. Zero sampled plan lowering or sink
access does not mean zero cost. Profiled StreamFusion runs record 1,189 / 135 plan/Calc invocations
in memory and 1,427 / 252 on RocksDB.

The profiles retain substantial startup/JIT, global-window/join work and RocksDB execution.
Java allocation samples are dominated by byte arrays and Flink row-memory descriptors; these
samples do not measure every native allocation. The checkpoint stops after the general memory
improvements and does not specialize execution for Q5's SQL or event distribution.

Raw logs, exact commands/classpaths, CPU/build metadata, summaries, JFR recordings, CPU and
allocation collapsed stacks, per-engine flame graphs, differential flame graphs and category
scripts are retained under `streamfusion-nexmark-benchmarks/target/measurements/q5/`.
The `5587afa0` directory contains backend-specific `1000000` and `profiles-2000000` runs,
plus the failed memory diagnostic under `hashmap/capacity-10000000`.
Earlier revisions retain their completed measurements and failed diagnostics. In particular,
the two-million-event profile at `084d5b9d` failed during join decoding; the matched profiles
above pass after the paged-decoding fix with unchanged JVM and Flink settings. Generated
artifacts are intentionally not checked into Git.
