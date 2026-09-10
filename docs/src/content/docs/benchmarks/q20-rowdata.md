---
title: Q20 RowData release comparison
description: Default regular-join admission, exact conformance, release performance and memory limits.
---

Original Q20 now passes ordinary whole-plan admission with Flink's default two-input join.
Release `b03765be0d87b3298a182716d632b4739312cd35` joins bids to category-10 auctions and
preserves the original projection and blackhole schema. Catalog aliases only label positional
sink columns. The existing shared native join/Calc runtime now serves both `StreamExecJoin`
and binary `StreamExecMultiJoin`; this checkpoint changes admission and validation, with no
new native algorithm or upstream operator patch.

The benchmark's established multi-join-enabled preset previously hid default regular-join
fallback. These measurements explicitly set the existing Flink option
`table.optimizer.multi-join.enabled=false` in both engines. Production selection requires no
such override: false is Flink's default. Admission also checks active configuration when an
option is absent from persisted metadata, preserving fallback for asynchronous state,
mini-batching and changelog-state wrapping.

## Unprofiled release measurements

Each backend uses three fresh-JVM pairs in F/SF, SF/F, F/SF order. Times are end-to-end seconds;
MAD is median absolute deviation. The throughput ratio divides Flink median time by
StreamFusion median time. Every measured fork is retained; profiled timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 6.293 [6.061, 12.954]; 0.232 | 15.958 [6.714, 19.686]; 3.728 | 0.394× |
| rocksdb | 1,000,000 | 27.896 [21.676, 42.901]; 6.220 | 7.102 [6.979, 8.467]; 0.123 | 3.928× |

All three in-memory pairs favor Flink. Both engines vary substantially and their ranges
overlap; StreamFusion's 0.394× median throughput is a regression in this comparison.
All three RocksDB pairs favor StreamFusion, with disjoint ranges and a 3.928× median ratio.
These bounded local runs establish neither a universal speedup nor a performance ceiling.

Separate in-memory profiles at 1.5M and 2M events completed on Flink but failed on StreamFusion
when its next join workspace reservation exceeded the existing Flink allowance:

- At 1.5M, mutation encoding requested another 192,192 bytes with 6,475,334 bytes already
  reserved by that consumer and 33,200 bytes available.
- At 2M, the in-flight batch state requested 6,388,692 bytes with zero already reserved by
  that consumer and 6,270,712 bytes available.

These are demonstrated native budget limits under this configuration. They do not diagnose
process OOM, establish a precise event-count ceiling, or supply performance ratios for the
incomplete pairs. Both engines completed the smaller 1.1M profile without changing the allowance.
The larger failures remain in the artifacts; memory admission was not bypassed.

## Correctness, computation and acceleration

The shared binary-join prerequisite passed 76 cases against actual Flink regular-join and
MultiJoin functions: 16 metric/changelog cases, 36 checkpoint/rescaling cases and 24 channel
recovery cases. These cover equality, range and timestamp-offset predicates, wide residuals,
all four RowKinds, ordered changelog bytes and timestamp envelopes, complete metric surfaces,
latency semantics, both backends, aligned/unaligned checkpoints, canonical backend switches,
1-to-2-to-1 rescaling, incremental SST reuse and actual Arrow IPC channel replay.

Admission adds 19 focused unit checks and 17 original-query integration cases. The latter
include catalog equivalence, original SQL with both optimizer choices and backends, and
collecting/blackhole execution at 50,000 events with parallelism 1/4. Complete collected
changelog/result digests and blackhole counts match. Independent join inputs may interleave
output differently; controlled operator tests compare the complete ordered changelog.
Topology guards verify a shared join/Calc state owner behind two Arrow IPC input edges.

DataFusion evaluates supported residual expressions and Calc stages; Arrow kernels gather
outputs. Custom join state transitions preserve Flink's multiset/retraction behavior and
key-group checkpoint contract. DataFusion's append-input symmetric hash join does not provide
that RowKind/state contract. The exception is documented in [joins](/StreamFusion/operators/joins/).
State access and dirty writes remain batched, and adjacent native stages share Arrow buffers
without an intermediate JVM crossing. This checkpoint retains that existing implementation.
Outer, semi/anti, cross, unique-key, unsupported residual, TTL and unsupported configuration
subsets retain precise whole-plan fallback; see the operator page for the full contract.

Every completed native fork reports acceleration and positive native-plan activity, while
Flink reports zero native activity. Blackhole output counts match in every completed pair.
Counts supplement collecting validation and do not prove byte parity at benchmark scale.

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 186,355 | 881 / 304, 857 / 296, 833 / 288 |
| rocksdb | 1,000,000 | measured, three forks | 186,355 | 1517 / 516, 1766 / 620, 1541 / 524 |
| hashmap | 1,100,000 | profile only | 217,132 | 930 / 320 |
| rocksdb | 2,000,000 | profile only | 378,718 | 3005 / 1012 |

## Separate mixed JVM/native profiles

Complete pairs use 1.1M in-memory events and 2M RocksDB events, longer than the 1M measured
cases. JFR, CPU/allocation collapsed stacks, per-engine flame graphs and differential flame
graphs are retained locally. Incomplete 1.5M/2M in-memory pairs are excluded from CPU comparisons.

Shares use all process CPU samples. Categories are inclusive and overlap: JNI includes
execution beneath it, and DataFusion frames include stream wrappers around custom state
adaptation. Zero means no matching sample. These percentages are not elapsed-time ratios
and cannot alone attribute the measured speedup to a particular change.

| Inclusive CPU sample category | hashmap 1.1M F / SF (%) | rocksdb 2M F / SF (%) |
| --- | ---: | ---: |
| native regular join | 0.000 / 3.707 | 0.000 / 4.578 |
| native join state helpers | 0.000 / 0.750 | 0.000 / 1.389 |
| Arrow row encoding | 0.000 / 1.041 | 0.000 / 0.663 |
| memory budget callbacks | 0.000 / 0.458 | 0.000 / 0.474 |
| garbage collection | 8.137 / 3.957 | 2.203 / 3.631 |
| row copy | 15.053 / 5.081 | 10.335 / 6.441 |
| RowData-to-Arrow writing | 0.000 / 2.041 | 0.000 / 2.021 |
| Arrow C Data / JNI, inclusive | 0.000 / 6.456 | 0.000 / 14.462 |
| native plan lowering | 0.000 / 0.042 | 0.000 / 0.000 |
| DataFusion frames, inclusive | 0.000 / 4.290 | 0.000 / 5.841 |
| DataFusion functions and expressions | 0.000 / 0.541 | 0.000 / 0.600 |
| Arrow gather | 0.000 / 0.250 | 0.000 / 0.284 |
| Arrow output view access | 0.000 / 1.125 | 0.000 / 1.579 |
| source polling, inclusive | 24.654 / 18.701 | 17.113 / 23.713 |
| RocksDB, inclusive | 0.000 / 0.000 | 39.766 / 6.189 |
| RocksDB index reads | 0.000 / 0.000 | 1.339 / 0.000 |
| RocksDB decompression | 0.000 / 0.000 | 0.796 / 0.726 |
| native artifact loading | 0.000 / 1.624 | 0.000 / 1.895 |
| JIT compilation | 38.527 / 40.900 | 18.417 / 35.680 |

CPU samples (Flink / StreamFusion): hashmap 2,458 / 2,401; RocksDB 5,902 / 3,167.

Native join execution accounts for 3.707% / 4.578% of native-run samples (hashmap / RocksDB),
and its state helpers for 0.750% / 1.389%. These samples identify no dominant native compute
kernel that justifies another algorithm rewrite. Memory callbacks remain below 0.5%, and
plan lowering is negligible. Q20 has no join residual beyond equality; its filter and projections
still execute through DataFusion. No query-specific equality shortcut was added.

Source polling, JVM compilation and boundary work remain substantial. Native-run JIT shares
are 40.900% in memory and 35.680% with RocksDB. RocksDB JIT sample counts are close between
engines (1,087 / 1,130), despite different percentages because total samples differ. Storage
calls account for 39.766% of Flink's RocksDB profile and 6.189% of StreamFusion's. This supports
retaining batched native state access, but does not isolate the cause of the measured ratio.
The in-memory profile does not establish why the unprofiled forks vary so widely.

Further general work could reduce retained join workspace or boundary overhead, with new
measurements and parity checks. Those opportunities are not exhausted. The observed memory
limits remain material; this checkpoint makes no larger-capacity or universal performance claim.

For RocksDB profiles only, a launcher loads the verified plugin through JVM `System.load`
before benchmark main so async-profiler can resolve its symbols. Measured forks never use
that launcher. Profiled throughput is excluded from the performance table.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB uses Flink's default full/private-file checkpoint strategy.
No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing
and build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native
CPU features, frame pointers and profiling symbols without reducing optimization. CPU baseline
fingerprint: `44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `5ccf5d0e2760506b9123d7cb7ef54d14eb0f9dd5a4f0a3eedd723e7e15113823`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q20/b03765be/`. Failed cases remain recorded
alongside completed cases. Longer profiles are neither unprofiled performance comparisons nor
proof of restore capacity at their event counts. Other deployment sizes and mini-batch mode
are outside this measurement.
