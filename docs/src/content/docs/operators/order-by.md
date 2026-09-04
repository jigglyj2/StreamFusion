---
title: ORDER BY
description: Acceleration coverage and fallback behavior for Flink SQL ORDER BY.
sidebar:
  order: 12
---

**Current status:** Partially accelerated. Streaming `ORDER BY ... LIMIT/OFFSET` plans that Flink
lowers to `StreamExecSortLimit` use native Top-N state. Time-ascending streaming sorts that Flink
lowers to `StreamExecTemporalSort` are also native. Other complete sorts remain on Flink.

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

Temporal `ORDER BY` is necessarily global. Flink supplies a singleton distribution, and the native
translator enforces parallelism and max parallelism one. There is therefore no meaningful
key-group redistribution for this operator: scaling it above one would violate total ordering.
The planner retains any other full, unbounded global `ORDER BY` on Flink.

## Implementation

The finite path reuses the native Top-N protobuf node. Temporal sort has its own versioned protobuf
node, persistent native processor, raw keyed state, and timer service. It stores the secondary keys
in Arrow's order-preserving row encoding with the planned direction and null placement, so firing a
timer uses one stable byte-key sort and one final Arrow decode rather than materializing and taking a
second batch. Java constructs the physical plan and owns watermarks, barriers, distribution,
recovery, and metric publication; Arrow C Data crosses only at the fused-plan edge. This follows
Comet's distinct replacement-node and protobuf control-plane model. A future bounded full-sort path
can use DataFusion's parallel, spill-capable sort while Flink retains distribution and boundedness
decisions.

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

See the [Flink 2.3 ORDER BY documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/).
