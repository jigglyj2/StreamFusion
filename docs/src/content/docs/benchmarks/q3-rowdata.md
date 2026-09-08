---
title: Q3 RowData release comparison
description: Corrected cold-initialization measurements and mixed runtime profiles on both state backends.
---

Q3's synchronous binary inner equi join is admitted by the ordinary planner with in-memory
and default RocksDB state. On this machine, the ten-million-event RowData-to-blackhole
comparison has small median throughput advantages over Flink on both backends. At one million
events, StreamFusion is slower. These are end-to-end job measurements, not steady-state rates.

## Measurements, September 8, 2026

Each cell gives median seconds, followed by the observed minimum–maximum and median absolute
deviation (MAD). Each engine ran in three fresh, unprofiled JVMs; engine order alternated
Flink/StreamFusion, StreamFusion/Flink, Flink/StreamFusion.

| Backend | Input events | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | ---: | --- | --- | ---: |
| In-memory | 1,000,000 | 5.005 [5.003–5.140]; 0.003 | 5.795 [5.781–5.806]; 0.011 | 0.864× |
| RocksDB | 1,000,000 | 5.163 [5.125–5.192]; 0.029 | 6.054 [6.009–6.095]; 0.041 | 0.853× |
| In-memory | 10,000,000 | 12.806 [12.012–12.968]; 0.162 | 12.349 [10.728–12.361]; 0.013 | 1.037× |
| RocksDB | 10,000,000 | 11.405 [11.243–11.640]; 0.162 | 11.090 [10.949–11.139]; 0.049 | 1.028× |

The ten-million-event median rates are approximately 780,876 versus 809,804 input events/s
for memory and 876,817 versus 901,716 for RocksDB, respectively Flink and StreamFusion.
The memory ranges overlap. The RocksDB ranges do not overlap in these three pairs, but a
2.8% observed advantage on one host is not a general performance guarantee. Smaller jobs
expose StreamFusion's fixed initialization and planning costs.

All native runs report `Accelerated: yes` and nonzero native activity. Native plan invocation
ranges are 833–856 (memory, one million), 850–901 (RocksDB, one million), 7,509–7,565 (memory,
ten million) and 7,504–7,549 (RocksDB, ten million). Corresponding Calc batch ranges are
288–296, 294–318, 2,516–2,536 and 2,512–2,528. Flink runs report zero native invocations.
These counters measure native batches/streams; operator I/O metrics still count logical records.

## Method and correctness boundary

The clean benchmark checkout is `d5b5de27460154805e6576f11066a242aceccdaf`. Both engines use
the deterministic bounded Nexmark RowData generator, the same Q3 SQL and Flink's unmodified
blackhole sink. The collecting-sink comparison separately verifies the complete sorted
changelog bytes/hash at 10,000 events and parallelism four on both backends. Generated join/Calc
tests cover all four RowKinds and the complete default Flink metric surface. Shared operator
harnesses cover canonical backend switching, aligned/unaligned checkpoints, incremental SST
reuse, rescaling and actual channel replay. This does not imply admission for other join subsets.

The machine is a WSL2 Linux VM on an Intel Core i7-12650H, reporting 16 logical processors,
about 7.6 GiB RAM and 2 GiB swap. Java is 24.0.2+12-54. Both engines use parallelism four,
`-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`, one GiB of Flink managed
memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, one-second exactly-once
checkpoints, filesystem checkpoint storage and no restart. Mini-batching is disabled.

Rust uses release optimization, the build host's native CPU features, frame pointers and
profiling information. Runtime libraries carry verified CPU metadata and checksums. ELF debug
information is separate; profiled and unprofiled forks load the same execution library.
The core artifact SHA-256 is `5c2a82b83b5d6d476ca3de414d72c4899418467a2c0333c8ea340ead18ee127f`;
the state artifact is `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

The timer includes table setup, planning preflight, native initialization for StreamFusion,
local cluster startup, execution and cleanup. It excludes JVM launch and argument parsing;
whole-process wall times are retained separately. Diagnostic counter access is Java-only,
so it does not preload native code into the Flink baseline or move StreamFusion initialization
outside the timer. Earlier measurements on the general RowData benchmark page predate this
correction and are labelled historical.

## Separate CPU profiles

Each engine/backend also ran a separate 30-million-event fork with async-profiler 4.5,
10 ms CPU sampling, Java non-safepoint sampling, native DWARF/frame-pointer unwinding and JFR
output. Matching debug-link symbols were installed before profiling. Allocation samples used
a two-MiB interval. Profiled timings are excluded from the measurements above.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Row serializer/binary copying | 33.041% | 15.936% | 31.261% | 15.075% |
| RowData-to-Arrow writing | 0% | 6.934% | 0% | 6.569% |
| Arrow C Data/JNI, including downstream execution | 0% | 7.322% | 0% | 10.803% |
| DataFusion execution | 0% | 4.070% | 0% | 4.642% |
| Native join, including its state work | 0% | 2.713% | 0% | 5.557% |
| RocksDB calls and their descendants | 0% | 0% | 7.539% | 6.988% |
| Arrow row-view/sink access | 0% | 0.129% | 0% | 0.097% |
| StreamFusion artifact loading | 0% | 0.420% | 0% | 0.448% |
| Source polling, including its downstream chain | 73.825% | 69.355% | 72.671% | 64.925% |
| JIT compiler threads | 11.334% | 15.462% | 11.870% | 15.017% |

The denominators are all process CPU samples: 10,720, 9,287, 11,036 and 10,275 respectively.
Categories overlap and must not be added. Plan lowering was not sampled in these forks;
that means insufficient sampled evidence for its cost, not zero work. Native profile runs
record 22,241 plan invocations/7,424 Calc batches for memory and 22,386/7,480 for RocksDB.

The source-side chain remains dominant, with substantial row copying and JVM compilation.
The native join and DataFusion computation account for relatively small shares. The Q3
optimizations therefore stop at reusable source-field projection, batched memory admission,
equivalent RocksDB configuration and corrected artifact loading. Further substantial gains
should target shared source/boundary costs while preserving Flink ownership and Arrow batches.

Raw logs, exact commands/classpaths, CPU/build metadata, summaries, JFR recordings, CPU and
allocation collapsed stacks, per-engine flame graphs and differential flame graphs are retained
under `streamfusion-nexmark-benchmarks/target/measurements/q3-cold/`. The analysis scripts and
inclusive-category regular expressions are retained there as well. Generated profiles are not
checked into Git.
