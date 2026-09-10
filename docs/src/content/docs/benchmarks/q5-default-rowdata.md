---
title: Q5 default WindowJoin release comparison
description: Bounded payload storage, default-optimizer Q5 measurements, and remaining performance work.
---

At release code `74995d3d0c0731d3974768f6c16e77dcc15a9991`, Q5 accelerates with Flink's default
`table.optimizer.multi-join.enabled=false`. Bounded payload pages remove the demonstrated
window-join retained-state failure: all one- and two-million-event measured forks complete, as
do separate four-million-event profiles and collecting-sink validation on both backends.

At two million events, median throughput is 1.6% lower in memory and 3.9% higher on RocksDB, whose
wide timing range makes the apparent gain uncertain. Both backends are slower at one million.
These results establish a capacity improvement, not a general speedup or a claim that all
reasonable performance work is exhausted. Global-window state-read duplication remains a concrete
follow-up from the profile inspection.

The [older Q5 report](/StreamFusion/benchmarks/q5-rowdata/) uses the enabled multi-join optimizer
and different memory-consumer weights. Its measurements must not be combined with these results.

## Measurements, September 10, 2026

Each case uses three fresh unprofiled JVMs per engine, alternating Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation. Timing is end-to-end:
setup, EXPLAIN, native initialization, cluster startup, execution and cleanup; JVM launch,
argument parsing and compilation are excluded. These are local measurements, not steady-state
throughput guarantees.

| Backend | Input events | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | ---: | --- | --- | ---: |
| In-memory | 1,000,000 | 6.327 [6.286–6.349]; 0.022 | 6.522 [6.517–6.668]; 0.004 | 0.970× |
| In-memory | 2,000,000 | 7.541 [7.370–7.541]; 0.000 | 7.660 [7.548–7.981]; 0.111 | 0.984× |
| RocksDB | 1,000,000 | 7.366 [7.308–7.424]; 0.057 | 7.910 [7.732–7.974]; 0.064 | 0.931× |
| RocksDB | 2,000,000 | 9.907 [9.399–16.822]; 0.508 | 9.534 [9.517–10.762]; 0.017 | 1.039× |

StreamFusion loses all three pairs for both one-million-event cases and for two million in
memory; their engine ranges do not overlap. At two million on RocksDB, StreamFusion wins two
pairs and loses one. Flink ranges from 9.399 to 16.822 seconds, so StreamFusion's 3.9% median throughput
advantage does not establish a reliable RocksDB speedup. No profiled timing enters this table.

All 24 measured runs emit five blackhole records. Every StreamFusion EXPLAIN reports acceleration
and every native counter is positive; Flink reports zero native activity. Invocation counts by fork:

| Case | Shared native-plan batches | Native Calc batches |
| --- | --- | --- |
| In-memory, 1M | 644 / 631 / 623 | 73 / 72 / 68 |
| In-memory, 2M | 1185 / 1185 / 1179 | 132 / 132 / 131 |
| RocksDB, 1M | 769 / 775 / 779 | 132 / 131 / 128 |
| RocksDB, 2M | 1417 / 1411 / 1431 | 249 / 250 / 253 |

These invocation counters are distinct from Flink's logical-record I/O metrics.

Both engines use the same deterministic Nexmark RowData source and original SQL, the unmodified
Flink blackhole sink, parallelism four, UTC, disabled mini-batching, one-second exactly-once
checkpoints, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30` consumer weights.
JVM settings are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`, with the
required `java.nio` opening. No Kafka services or connector benchmarks are involved.

The machine is an Intel Core i7-12650H under WSL2, Linux 6.18.33.2, with OpenJDK 24.0.2. The clean
measurement checkout builds Rust with release optimization, native CPU features, frame pointers
and separate profiling symbols. CPU baseline is `44dd0ad765af32a3`. Core native SHA-256 is
`3cc1abe1b2341f50c354949a8c4f7008dc2979ec4081d0fb59c0e954d7f7c86a`; RocksDB native SHA-256 is
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

Forty-eight focused Rust window-join tests pass with both backends available, including a
20,003-row left window with only 4 MiB remaining, exact duplicate-pair order, output ownership,
malformed page framing, old indexed-state restore and cancellation followed by cross-backend
recovery. Twenty-eight focused Java tests cover runtime parity, complete metric surface,
rescaling, aligned/unaligned channel recovery and selected SQL. Eight opt-in official Q5 cases
compare collecting-sink bytes and blackhole counts at 20,000 events, both backends, parallelism
one/four and both multi-join settings.

Additional separate fresh-JVM collecting-sink runs at four million events and parallelism four
match all five result records and materialized results on both backends. Every output SHA-256 is
`6bac5b936f972ff1878172fcc7bf67153321641a26c5e6c3ef81d4a2d8de3230`. Fixed-arrival operator tests
compare ordered changelog bytes; independent jobs may interleave windows differently. Collecting
runs validate results and are excluded from the blackhole performance comparison.

## Longer CPU profiles

Separate four-million-event async-profiler 4.5 runs complete for both engines on both backends,
with 10 ms CPU sampling, Java non-safepoint sampling, native DWARF unwinding and JFR output.
Each emits five records. StreamFusion's shared native-plan/Calc invocation counts are 2,271/257
in memory and 2,784/501 on RocksDB. RocksDB profiles preload the same verified native plugin to
expose its symbols; unprofiled measurements do not preload it.

The following inclusive shares use all process CPU samples as denominator: 3,974/3,612 samples
for Flink/StreamFusion in memory and 5,819/4,856 on RocksDB. Categories overlap: JNI includes
underlying computation and DataFusion includes stream execution. Sampling and native unwinding
can miss frames; zero means no matching sample, not zero execution cost.

| CPU category | Memory Flink / StreamFusion | RocksDB Flink / StreamFusion |
| --- | ---: | ---: |
| Source polling, including downstream calls | 30.40% / 27.21% | 22.31% / 20.57% |
| RowData copying | 14.19% / 6.04% | 10.41% / 4.14% |
| RowData-to-Arrow writing | 0.00% / 2.49% | 0.00% / 1.63% |
| Arrow C Data / JNI, inclusive | 0.00% / 19.10% | 0.00% / 38.98% |
| Native plan lowering | 0.00% / 0.00% | 0.00% / 0.02% |
| DataFusion execution | 0.00% / 17.03% | 0.00% / 16.23% |
| Arrow-backed output access | 0.00% / 0.00% | 0.00% / 0.00% |
| JVM JIT compilation | 28.46% / 32.34% | 20.50% / 24.24% |

StreamFusion's observed window-join adapter share is 1.52% in memory and 2.49% on RocksDB; its
closing path is 0.42% and 1.22%. Global-window execution is larger, at 13.95% and 12.32%, with
state lookups, hashing, allocation and timer work beneath window firing. RocksDB frames account
for 34.49% of Flink's and 23.87% of StreamFusion's process samples. Some storage stacks omit Rust
callers, so these are observed shares rather than exhaustive operator attribution.

Inspection found that the global-window firing adapter duplicates each requested state key in
its deduplication map and its ordered read list. Reducing that duplication is a general next
optimization; neither these profiles nor the throughput measurements establish its benefit yet.
Large JIT and source shares also limit conclusions about steady-state compute. The measured
capacity is not an unlimited-memory guarantee: retained state, complete right windows and
DataFusion workspace remain subject to Flink's original allowance.

## Earlier capacity evidence

At `a4dcc33d`, whole-window decode staging failed at one million events on both backends.
`a47c7340` introduced bounded left decoding and completed three measured pairs at one million:
6.411/6.859 seconds median Flink/StreamFusion in memory, and 9.066/8.570 on RocksDB. Its longer
in-memory attempts still failed on `WindowJoin[45]`: another 225,475 bytes were denied with
61,803,681 already held and 27,091 available at two million events. A separate unprofiled
1.25-million-event diagnostic reproduced the retained-state failure. The payload-page change
addresses that entry overhead; the new runs above complete through four million.

Earlier and current results are separate measurements; changes in Flink's baseline timings
prevent interpreting their raw elapsed-time difference as the isolated effect of this change.
Raw evidence remains under `streamfusion-nexmark-benchmarks/target/measurements/q5-default/`.
The `74995d3d/` directory contains `hashmap-1m`, `rocksdb-1m`, `hashmap-2m`, `rocksdb-2m`, both
`*-4m-profile` directories and `collecting-validation.json`. Metadata, every run, per-engine JFRs,
CPU/allocation collapsed stacks, flame graphs, differential flame graphs and category definitions
are retained. Historical failures and profiles remain in the `a4dcc33d/` and `a47c7340/` directories.
