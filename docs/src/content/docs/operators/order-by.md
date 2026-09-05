---
title: ORDER BY
description: Acceleration coverage and fallback behavior for Flink SQL ORDER BY.
sidebar:
  order: 12
---

**Current status:** Accelerated for bounded and finite-stream full sorts, streaming
`ORDER BY ... LIMIT/OFFSET`, and time-ascending temporal sorts. Flink lowers these to
`BatchExecSort` or `StreamExecSort`, `StreamExecSortLimit`, and `StreamExecTemporalSort`
respectively. A genuinely unbounded global full sort remains invalid in streaming SQL rather than
being approximated.

## SQL example

```sql
SELECT auction, price
FROM bid
ORDER BY price DESC
LIMIT 100 OFFSET 10;
```

## Acceleration and fallback

Finite streaming sort-limit accepts the same Flink-valid order-key types, null placement,
ascending/descending directions, changelog strategies, state backends, and recovery contract as
[Top-N](../top-n/).

Bounded full sort is selected when Flink produces `BatchExecSort` or `StreamExecSort`, including
the internal `__table.exec.sort.non-temporal.enabled__` bounded-stream mode used by the parity and
Nexmark harnesses. It accepts every Arrow-supported payload type. Ordering fields accept every
Flink-comparable scalar, array, and row type, including decimal and temporal values, nested nulls,
floating-point NaN, and signed zero. Map, multiset, raw, symbol, and descriptor values remain valid
opaque payload fields but fall back if used as order fields because Flink has no exact comparator
contract for them. Ascending/descending direction and null placement are preserved per field.

INSERT and UPDATE_AFTER add one occurrence to a counted row multiset; UPDATE_BEFORE and DELETE
remove one. The terminal output is therefore the same sorted INSERT-only relation Flink emits for
an updating bounded input, including duplicates. A missing retraction fails with Flink's
`RowData not exist!` contract rather than silently changing the result.

Temporal sort accepts Flink's event-time and processing-time forms. The first order field must be
the ascending time attribute. Secondary fields support every Flink-comparable scalar, array, and
row type with the planned direction and null placement; map, multiset, and raw values remain valid
payload fields but cannot be order fields. The complete Arrow payload, including nested collections
and `NULL`, is stored without a RowData conversion. Although the native runtime preserves every
input RowKind, Flink 2.3 only constructs this physical node for insert-only input.

Rows and timers use the selected native memory or direct RocksDB backend. Aligned and unaligned
checkpoints preserve pending timestamp groups; canonical savepoints move them between both
backends, and RocksDB checkpoints reuse unchanged SSTs. Event-time rows at or behind the last fired
timestamp are dropped exactly where Flink drops them. Processing-time rows are grouped by Flink's
millisecond timer boundary.

The operator keeps Flink's logical-record I/O counters and task timing/rate metrics. Its
`StreamFusion` subgroup additionally reports processed batches and rows, output RowKinds, state
read/write batches, timer registration/firing/deletion, late-record drops, pending event- and
processing-time timers, watermark latency, managed native memory, and checkpoint/savepoint/restore
counts, bytes, timings, failures, and incremental uploaded/reused bytes.

Temporal and bounded full `ORDER BY` are necessarily global. Flink supplies a singleton
distribution, and the native translators enforce parallelism and max parallelism one. There is
therefore no meaningful key-group redistribution for either operator: scaling above one would
violate total ordering.

## Implementation

The finite limit path reuses the native Top-N protobuf node. Bounded full sort has a distinct
versioned protobuf node and persistent native processor. Each incoming Arrow batch is row-encoded
once, deduplicated with ahash, read with one backend batch call, and committed with one mutation
batch. Memory state keeps opaque bytes and the optional RocksDB component calls `multi_get` and one
`WriteBatch` directly in Rust; neither data path crosses JNI for state access. Counts and complete
Arrow rows use the canonical key-group format. At end of input, unique rows are decoded once,
ordered by the shared Flink comparator, and emitted in managed 16,384-row Arrow batches. The
operator advertises Flink's internal-sort capability so the runtime does not insert a second
`SortingDataInput` ahead of the native sorter.

Temporal sort has its own versioned protobuf node, persistent native processor, raw keyed state,
and timer service. It stores the secondary keys
in Arrow's order-preserving row encoding with the planned direction and null placement, so firing a
timer uses one stable byte-key sort and one final Arrow decode rather than materializing and taking a
second batch. Java constructs the physical plan and owns watermarks, barriers, distribution,
recovery, and metric publication; Arrow C Data crosses only at the fused-plan edge. This follows
Comet's distinct replacement-node and protobuf control-plane model.

## Benchmark evidence

The September 4, 2026 release/native-CPU run at commit `1e1e98d` used separate JVMs, a 2 GiB heap,
parallelism four, one-second exactly-once checkpoints, and three alternating forks per engine. On
100,000 events with in-memory state, Flink's median was 4.756 s (4.668–5.240 s) and StreamFusion's
was 5.260 s (5.222–5.423 s), or 90.4% throughput parity. The native path executed eight Temporal
Sort batches in each measured fork. This isolated topology must cross both RowData/Arrow boundaries;
the final mixed-stack profile attributes 5.1% of CPU samples to Temporal Sort and 9.2% inclusively
to Arrow/native boundary work, so the remaining memory gap is not an operator-local sort loop.

The RocksDB workload used 20,000 events because Flink repeatedly rewrites its growing
`List<RowData>` value. Its median was 24.669 s (22.975–40.518 s, retaining a storage outlier) versus
4.819 s (4.728–5.375 s) for direct native RocksDB, a 5.12x StreamFusion gain. Every fork produced
the same 18,400-row changelog and ordered digest. RocksDB occupied 62.8% of Flink CPU samples and
2.1% of StreamFusion samples. Sampled JVM allocation was 87.11 GB versus 0.65 GB; sampled native
allocator traffic was 89.35 GB versus 3.65 GB. The in-memory JVM allocation totals were essentially
equal at 0.952 GB and 0.949 GB.

CPU JFRs use Java non-safepoint sampling and DWARF/frame-pointer native unwinding. Separate Java and
native-allocation JFRs, collapsed stacks, flame graphs, steady-state views, and differential flame
graphs are retained under
`streamfusion-nexmark-benchmarks/target/profiles/temporal-sort/1e1e98d/`. Profiling exposed repeated
growth of the canonical state buffer; exact checked pre-sizing removed that reallocation stack.
The remaining encoding allocation is the one durable opaque state value, and its native allocation
share fell from 8.0% to 2.9%. Profiler timings are excluded from the throughput results above.

The September 4, 2026 bounded-full-sort release/native-CPU run used the Kafka-free NEXMark RowData
boundary, 250,000 events, parallelism four upstream of Flink's required singleton exchange, and
three fresh JVM forks per engine/backend. Every fork emitted the same 230,000 ordered rows with raw
SHA-256 `be936c9af5af60dff75147361e571e97284a7c2c5837d47e77758391d60d9a85` and ordered SHA-256
`218058bfcf41f3863c6b7c5522f7d4bb1f4b484feab12332e43acdd68a439852`. In memory, Flink's median
was 5.642 s (MAD 0.051 s, range 5.488–5.693 s) and StreamFusion's was 6.154 s (MAD 0.071 s, range
5.970–6.225 s), or 91.7% end-to-end throughput parity. RocksDB medians were 5.466 s for Flink (MAD
0.074 s, range 5.392–6.034 s) and 7.392 s for StreamFusion (MAD 0.245 s, range 7.148–8.124 s), or
73.9% parity. Flink's bounded sort does not itself use keyed state, while the StreamFusion RocksDB
case deliberately persists the counted multiset, so that comparison includes the requested native
backend durability path.

Mixed CPU profiles cover the complete source, RowData/Arrow boundary, singleton exchange,
JNI/native state and sort, Arrow/RowData boundary, and sink for both engines and backends. They
first exposed an unintended Flink `SortingDataInput` ahead of the native operator; declaring native
internal-sort ownership removed that complete duplicate sort and reduced the RocksDB smoke time
from 8.462 s to 6.508 s. Final profiles contain no `SortingDataInput` samples. Separate Java and
native-allocation captures show source deserialization and result materialization dominating Java
allocation; bounded-sort native allocation is primarily durable row keys and terminal Arrow output,
with direct RocksDB adding its write-batch/memtable traffic. The native path reported 64 bounded-sort
batches in memory and 124 with RocksDB. JFRs, collapsed stacks, flame graphs, and differential
graphs are retained under `streamfusion-nexmark-benchmarks/target/bounded-sort-profiles-postfix/`;
profiler timings are excluded from the medians.

See the [Flink 2.3 ORDER BY documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/).
