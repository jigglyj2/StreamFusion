---
title: Q5 default WindowJoin release comparison
description: Bounded payload storage, default-optimizer Q5 measurements, and remaining performance work.
---

At release code `60eb188ce9470a8cf6685006da066acc926ce651`, Q5 accelerates with Flink's default
`table.optimizer.multi-join.enabled=false`. All one- and two-million-event measured forks complete,
as do separate four-million-event profiles and collecting-sink validation on both backends.
The local DataFusion buffer now reduces compute-chunk size when workspace admission is denied,
while preserving full input-key accounting and Flink's partial-flush boundaries.

Median throughput is 4.8% lower in memory at one million events and 5.4% lower at two million.
On RocksDB it is 21.1% higher at one million and 1.6% higher at two million, with overlapping
engine ranges for the latter. Flink's one-million-event RocksDB baseline is substantially slower
than in earlier comparisons; these runs do not isolate the performance effect of the admission
change. Q5 is accelerated and the demonstrated capacity failures have passing larger runs, but
it is not consistently faster than Flink or proven optimal.

The [older Q5 report](/StreamFusion/benchmarks/q5-rowdata/) uses the enabled multi-join optimizer
and different memory-consumer weights. Its measurements must not be combined with these results.

## Measurements, September 10, 2026

Each case uses three fresh unprofiled JVMs per engine, alternating Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation. Timing is end-to-end:
setup, EXPLAIN, native initialization, cluster startup, execution and cleanup; JVM launch,
argument parsing and artifact builds are excluded. Runtime JIT compilation is included. These
are local measurements, not steady-state throughput guarantees.

| Backend | Input events | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | ---: | --- | --- | ---: |
| In-memory | 1,000,000 | 6.404 [6.387–6.425]; 0.018 | 6.726 [6.665–6.838]; 0.061 | 0.952× |
| In-memory | 2,000,000 | 7.346 [7.317–7.623]; 0.029 | 7.762 [7.556–7.853]; 0.091 | 0.946× |
| RocksDB | 1,000,000 | 10.318 [10.044–11.407]; 0.274 | 8.522 [8.337–8.751]; 0.186 | 1.211× |
| RocksDB | 2,000,000 | 9.769 [9.264–9.840]; 0.071 | 9.617 [9.584–9.619]; 0.002 | 1.016× |

StreamFusion loses all three pairs for both in-memory cases. At one million their ranges do
not overlap; at two million they overlap. On RocksDB it wins all three one-million-event pairs
with disjoint ranges, and wins two of three two-million-event pairs with overlapping ranges.
The latter 1.6% median gain is weak evidence of a reliable speedup. No profiled timing enters
this table. Flink's one-million-event RocksDB median is higher than its two-million-event median,
which further limits extrapolation from these short end-to-end jobs.

All 24 measured runs emit five blackhole records. Every StreamFusion EXPLAIN reports acceleration
and every native counter is positive; Flink reports zero native activity. Invocation counts by fork:

| Case | Shared native-plan batches | Native Calc batches |
| --- | --- | --- |
| In-memory, 1M | 636 / 635 / 643 | 69 / 70 / 70 |
| In-memory, 2M | 1185 / 1189 / 1183 | 136 / 132 / 135 |
| RocksDB, 1M | 749 / 747 / 763 | 127 / 128 / 128 |
| RocksDB, 2M | 1435 / 1423 / 1423 | 255 / 253 / 253 |

These invocation counters are distinct from Flink's logical-record I/O metrics.

Both engines use the same deterministic Nexmark RowData source and original SQL, the unmodified
Flink blackhole sink, parallelism four, UTC, disabled mini-batching, one-second exactly-once
checkpoints, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30` consumer weights.
JVM settings are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`, with the
required `java.nio` opening. No Kafka services or connector benchmarks are involved.

The machine is an Intel Core i7-12650H under WSL2, Linux 6.18.33.2, with OpenJDK 24.0.2. The clean
measurement checkout builds Rust with release optimization, native CPU features, frame pointers
and separate profiling symbols. CPU baseline is `44dd0ad765af32a3`. Core native SHA-256 is
`84a8a9326346c4659abdf4d585137ffaaddc636f9aec3a05f95a8a95e4549d6d`; RocksDB native SHA-256 is
`fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.
Metadata retains complete JVM flags, upstream revisions and patches, classpath and artifact
properties. Flink's local patches are the permitted planner installation/class-loading and
post-StreamGraph resource-finalization hooks; operator algorithms, source and sink remain intact.

## General improvements and validation

The close adapter retains one decoded right window and processes ordered left pages through the
actual DataFusion join. It shares Arrow buffers and reclaims acknowledged left payloads only
after DataFusion EOF, preserving Flink's left-major order without extra JVM/native handoffs.
The complete right window still must fit its allowance.

The storage change appends immutable payload pages of at most 256 rows and normally 16 KiB,
sharing the partition/window prefix and ordered-tree entry across those rows. A native regression
stores 10,000 rows as 40 payload entries with less than 1 MiB of retained in-memory growth.
This reduces actual storage, without reducing accounting or changing Flink's allocation rules.
The [window-join contract](/StreamFusion/operators/window-join/) documents the versioned encoding,
old-state compatibility, wide-row handling and recovery behavior.

The preceding payload-page change passed forty-eight focused Rust window-join tests with both
backends available, including a 20,003-row left window with only 4 MiB remaining, exact duplicate-pair order, output ownership,
malformed page framing, old indexed-state restore and cancellation followed by cross-backend
recovery. Twenty-eight focused Java tests cover runtime parity, complete metric surface,
rescaling, aligned/unaligned channel recovery and selected SQL. Eight opt-in official Q5 cases
compare collecting-sink bytes and blackhole counts at 20,000 events, both backends, parallelism
one/four and both multi-join settings.

The adaptive-input change passes 33 focused Rust local-window tests, 19 Java parity/resource/
processing-pressure/selected-SQL tests and all eight official Q5 integration cases. The new
pressure regression compares ordered checkpoint partials with an unpressured run, verifies that
smaller compute slices emit no early partials, and retains hard denial when one row cannot fit.
Global firing also avoids a redundant deduplication map: a callback handles one timestamp frontier
with unique partition/slice requests. Its earlier focused validation passed 95 native window tests,
74 Java parity/metric/recovery tests and the eight Q5 cases. Neither change adds per-row JNI work.

Additional separate fresh-JVM collecting-sink runs at four million events and parallelism four
match all five result records and materialized results on both backends. Every output SHA-256 is
`6bac5b936f972ff1878172fcc7bf67153321641a26c5e6c3ef81d4a2d8de3230`. Fixed-arrival operator tests
compare ordered changelog bytes; independent jobs may interleave windows differently. Collecting
runs validate results and are excluded from the blackhole performance comparison.

## Longer CPU profiles

Separate four-million-event async-profiler 4.5 runs complete for both engines on both backends,
with 10 ms CPU sampling, Java non-safepoint sampling, native DWARF unwinding and JFR output.
Each emits five records. StreamFusion's shared native-plan/Calc invocation counts are 2,283/259
in memory and 2,783/501 on RocksDB. RocksDB profiles preload the same verified native plugin to
expose its symbols; unprofiled measurements do not preload it.

The following inclusive shares use all process CPU samples as denominator: 3,880/3,358 samples
for Flink/StreamFusion in memory and 5,512/4,733 on RocksDB. Categories overlap: JNI includes
underlying computation and DataFusion includes stream execution. Sampling and native unwinding
can miss frames; zero means no matching sample, not zero execution cost.

| CPU category | Memory Flink / StreamFusion | RocksDB Flink / StreamFusion |
| --- | ---: | ---: |
| Source polling, including downstream calls | 31.39% / 29.04% | 22.88% / 21.42% |
| RowData copying | 13.94% / 7.15% | 10.47% / 5.05% |
| RowData-to-Arrow writing | 0.00% / 2.35% | 0.00% / 1.82% |
| Arrow C Data / JNI, inclusive | 0.00% / 16.89% | 0.00% / 38.18% |
| Native plan lowering | 0.00% / 0.00% | 0.00% / 0.00% |
| DataFusion execution | 0.00% / 14.62% | 0.00% / 14.54% |
| Arrow-backed output access | 0.00% / 0.09% | 0.00% / 0.00% |
| JVM JIT compilation | 28.40% / 31.63% | 21.39% / 24.23% |

StreamFusion's observed window-join adapter share is 1.73% in memory and 2.16% on RocksDB; its
closing path is 0.39% and 1.18%. Local-window execution is 0.95% and 0.76%, and memory-budget
callbacks are 0.74% and 0.53%. Global-window execution remains larger, at 10.81% and 10.75%.
Detailed stacks show batched state reads, state removal, timer registration and allocation beneath
window firing. The removed duplicate read-key map is absent. RocksDB frames account for 34.33%
of Flink's and 25.48% of StreamFusion's process samples. Some storage stacks omit Rust callers,
so these are observed shares rather than exhaustive operator attribution.

The profiles do not demonstrate another isolated, inexpensive change that would erase the
in-memory regression. Further state-read/timer allocation work remains possible, but should be
justified and measured as a general state-runtime improvement. Large JIT and source shares limit
conclusions about steady-state compute. The measured capacity is not an unlimited-memory
guarantee: retained state, complete right windows and DataFusion workspace remain subject to
Flink's original allowance.

## Earlier capacity evidence

At `a4dcc33d`, whole-window decode staging failed at one million events on both backends.
`a47c7340` introduced bounded left decoding and completed three measured pairs at one million:
6.411/6.859 seconds median Flink/StreamFusion in memory, and 9.066/8.570 on RocksDB. Its longer
in-memory attempts still failed on `WindowJoin[45]`: another 225,475 bytes were denied with
61,803,681 already held and 27,091 available at two million events. A separate unprofiled
1.25-million-event diagnostic reproduced the retained-state failure. The payload-page change
addresses that entry overhead; the new runs above complete through four million.

At `74995d3d`, payload pages completed all measured jobs and four-million-event profiles and
collecting validation. At the subsequent `4afbcd6f`, the second two-million-event RocksDB
StreamFusion fork failed a local-window reservation: 591,107 additional bytes were denied with
166,781 available. That incomplete case has no valid median. Adaptive input admission has now
completed three measured pairs at that size and the larger validation above; failed evidence
is retained rather than replaced by successful reruns.

Earlier and current results are separate measurements; changes in Flink's baseline timings
prevent interpreting their raw elapsed-time difference as the isolated effect of a code change.
Raw evidence remains under `streamfusion-nexmark-benchmarks/target/measurements/q5-default/`.
The `60eb188c/` directory contains `hashmap-1m`, `rocksdb-1m`, `hashmap-2m`, `rocksdb-2m`, both
`*-4m-profile` directories and `collecting-validation.json`. Metadata, every run, per-engine JFRs,
CPU/allocation collapsed stacks, flame graphs, differential flame graphs and category definitions
are retained. Earlier evidence remains in `a4dcc33d/`, `a47c7340/`, `74995d3d/` and `4afbcd6f/`.
