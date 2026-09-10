---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Partial. Synchronous binary `INNER` joins represented by Flink's
`StreamExecJoin` or binary `StreamExecMultiJoin` use the shared native plan with in-memory or default RocksDB state when their complete
condition is covered by common equi keys and optional boolean combinations of direct
column/literal comparisons or null checks. Comparison/null-check operands may also use a
`TIMESTAMP(3)` column plus or minus a non-null literal day-time interval. Other computed residual
operands retain a specific workspace fallback. Both inputs must use non-unique multiset state.
Outer joins, TTL, mini-batching, async/changelog state, enabled
state-latency metrics, and checkpointing during channel recovery retain whole-plan fallback.
RocksDB requires its optional native component, a compatible verified CPU artifact, and supported
default backend settings. Unsupported settings retain their precise fallback reason.
Sources and sinks use the normal Arrow boundary adapters; join and downstream Calc exchange Arrow
directly within one native plan. Generated changelog/metric, rescaling, checkpoint and channel replay
tests cover this path. Q3 passes ordinary admission and collecting/blackhole integration on both
backends. The [Q3 release comparison](/StreamFusion/benchmarks/q3-rowdata/) records corrected
measurements and profiles for both, including small larger-run median gains and slower smaller runs.

The shared binary-join conformance matrix also runs against Flink's actual synchronous
`StreamingJoinOperator`, with non-unique inner-join state on both inputs. Seventy-six cases
across regular and MultiJoin oracles verify complete metrics/changelogs for equality, range,
timestamp-offset and wide predicates, plus backend-switch savepoints, 1→2→1 rescaling,
incremental SST reuse and actual aligned/unaligned Arrow channel replay. This verifies the
existing native runtime for the regular-join subset. Ordinary `StreamExecJoin` now selects it
with Flink's default optimizer settings; enabling the multi-join optimizer is not required.
Q20's [release comparison](/StreamFusion/benchmarks/q20-rowdata/) reports 3.928× median
throughput with RocksDB at 1M events, slower in-memory execution, and explicit native
in-memory budget limits in larger profiles.
Admission combines active and persisted configuration, so async state, mini-batching and
changelog-state wrapping cannot evade fallback when an option is absent from persisted metadata.

Streaming regular joins explicitly clear record timestamps in the version-3 native plan, matching
Flink's `StreamingJoinOperator`. Their Arrow output owns its RowKind and absent-timestamp envelope,
including when a bare join ends a native region before an exchange. A following Calc is not required
to detach input arrival ordinals. The existing DataFusion record-policy projection shares payload
buffers and reserves its metadata arrays; no Java row reconstruction or additional JNI crossing is
needed. Generic plan composition preserves that protocol version when older fragments are appended.

Generated tests compare bare joins and two joins in one native tree with the actual Flink operators:
ordered changelog bytes, record timestamps, all four RowKinds, 5,000-row fan-out, complete stage metrics
and control events, with Arrow inputs and native IPC inputs on both backends. Additional two-join
tests cover canonical backend switching, 1→2→1 rescaling, aligned/unaligned snapshots, incremental
SST reuse, and replay of an in-flight third-input Arrow frame through both restored stages.
These contracts apply to composed binary joins. A genuine three-input `StreamExecMultiJoin` remains
subject to whole-plan fallback until its common execution-plan and lifecycle integration is verified.
Original Nexmark Q23, with only the `dateTime` identifier quoted for Flink 2.3, passes ordinary
planning plus collecting-result and unmodified-blackhole-count parity on both backends at
parallelism 1 and 4 with 100,000 events. This uses Flink's default disabled multi-join optimizer;
an explicit enabled-optimizer test checks the precise three-input fallback. The pinned upstream
generator adds `FIRST_PERSON_ID` twice to bid bidders, so short runs can produce no matches;
the Q23 tests require positive output. The generator remains unmodified. The
[Q23 release comparison](/StreamFusion/benchmarks/q23-rowdata/) reports slower 100k runs,
a variable 1.311× RocksDB median at 500k, complete separate profiles, and an in-memory
budget failure at 500k. These bounded results do not establish a performance ceiling.

Regular-join capability checks and protobuf construction belong to the planner bundle, where
Flink's `JoinSpec` and Calcite classes are visible. Runtime operators remain in the runtime
bundle and receive the completed native plan. This boundary also applies to the binary MultiJoin
representation used by Q3–Q5; loading its translator from the runtime classloader would cause
whole-plan fallback in the packaged distribution even when flat-classpath SQL tests pass.

The inner window-join and cached CSV lookup subsets below are also admitted. Other join paths are retained for development and direct parity tests under
[architecture admission](/StreamFusion/development/architecture-admission/).

### Attached inner window joins

Supported `StreamExecWindowJoin` nodes now select the common native region with Flink's default
disabled multi-join optimizer. The node builds a native fragment; it does not construct the
legacy Java candidate-matching operator. Admission covers synchronous inner joins with scalar
payloads, compatible equality keys or no equality keys, UTC event time, and bounded residual
predicates. Memory and default RocksDB state are supported. Mini-batching and the unsupported
semantic/configuration subsets below retain precise whole-plan fallback.

Q5's original SQL is checked with ordinary selection, generated SQL parity, two-input Arrow
exchange topology, original Flink managed-memory bindings, complete stage metrics, and checkpoint
recovery. Separate collecting and unmodified-blackhole runs exercise both backends at parallelism
one and four. Release measurement and profiling of this default WindowJoin path remain pending;
earlier Q5/Q8 measurements enabled the multi-join optimizer and exercised a binary join instead.
Q8's complete query checkpoint remains separate from this operator admission.

The version-3 inner-window contract has explicit left/right native children, SQL schemas,
equality keys, per-key null filters, and a serialized residual expression. Adjacent native
operators compose in one DataFusion tree; window-join output is Arrow with INSERT row kinds and
absent record timestamps. Rust lowers the residual to a DataFusion `JoinFilter`, referencing only
the columns needed for computation. A DataFusion `CASE` skips residual evaluation for filtered
null keys, preserving Flink's null-key wrapper even when the residual could throw.

Current validation rejects non-inner joins, non-UTC event time, incompatible equality keys,
collection payloads, async state, and changelog-state wrapping. Attached window ends accept
Flink's epoch-millisecond `BIGINT` and Arrow `TIMESTAMP(3)`; ingestion reads either representation
directly. Scalar payloads and bounded primitive comparison, arithmetic, boolean, null-check and
conditional predicates are supported by the compute contract. Predicates requiring expanding
kernels, floating NaN guards, decimal conversion, timestamp-offset kernels or other unadmitted
workspaces retain precise fallback. Planner inspection uses the active table configuration
merged with the persisted operator configuration, including its time zone.

Closed-window computation uses DataFusion 55 `NestedLoopJoinExec`. A complete, admitted right
Arrow window is shared across bounded left pages in arrival order. Each pair uses the reusable
native input node: splitting the right input would change Flink's left-row/right-row emission
order. A private build-pool view retains the already accounted left page without charging its
shared buffers again; it rejects unrelated consumers or storage beyond that input. Each page
caps DataFusion's batch capacity at its maximum possible pair count, avoiding a full default-batch
workspace for a tiny result. Creating a join execution for each left page resets DataFusion's
build state while retaining Flink's duplicate order; no intermediate JNI handoff is introduced.

Coarse reservations cover candidate/filter/coalescer workspace before execution. Output receives
its existing buffer allowance through shared ownership, including non-zero-offset slices.
Retaining output competes for Flink's existing budget; denial occurs before the next kernel poll.
Cancellation releases temporary workspace while retained output remains valid and accounted for.
This is reservation-based admission for growing buffers, not an allocation-by-allocation ledger.

State appends individual payload entries and updates a 29-byte window header per incoming batch;
it does not rewrite a growing opaque partition value. Partition prefixes are length-framed,
window ends use Arrow row ordering, and each side preserves a stable arrival ordinal. Memory
and RocksDB both use ordered range reads and batched deletion. Flink partition hashing remains
separate; keyless shared joins use Flink's eight-byte empty `BinaryRowData` key.

A watermark closes one window at a time, decoding at most 256 left rows per page with a byte
limit derived from the existing memory allowance. Encoded copies and deletion keys no longer
grow with the complete left window. Right-side decode and candidate computation must still fit
their reservations. A left page's payload entries are deleted in a batch only after its DataFusion
output reaches EOF; the right state, header and timer remain until the final page completes.
The partial-close cursor is invocation-local, never persisted. Ordinary invocation EOF, end-input,
and checkpoint preparation do not fire windows. Failed or cancelled invocations prevent
checkpointing and reuse and recover the previous Flink checkpoint.
Late retractions drop before changelog validation, matching Flink; on-time retractions are rejected.
Maximum window-end sentinels and wrapping deadline arithmetic retain Flink's behavior.

Shared snapshots carry an `SFWF/2` operator-contract fingerprint in every key group. **Window joins
restore timer queues with their watermark reset to `Long.MIN_VALUE`, as Flink does.** They do not
use window aggregates' persisted union-operator clocks. This matters for replayed rows arriving
before the first restored watermark. The earlier, unadmitted `SFWF/1` shared snapshot contract,
unmarked legacy-handle snapshots, and mismatched operator contracts are rejected. Legacy
state-handle `SFWJ/2` migration remains separate from shared-runtime admission.

The binding exposes Flink's `leftNumLateRecordsDropped`/`leftLateRecordsDroppedRate`,
`rightNumLateRecordsDropped`/`rightLateRecordsDroppedRate`, and `watermarkLatency`, plus logical
stage I/O counters through the shared metric tree. Generated Flink harness comparisons exercise
the production shared Arrow/JNI runtime followed by a native calc on both backends. They compare
ordered serialized changelogs and record timestamps for duplicates, nullable payloads/keys,
INSERT/UPDATE_AFTER, late retractions, and watermark output. They also check stage logical-record
counts and compare the complete registered metric surface, operator identity, latency-marker
metrics, and runtime-dependent metric semantics against Flink using the same processing clock.
Keyed and keyless cases cover residual predicates, idle/active transitions, and late records on
both inputs. Recovery cases include empty and populated timer state, canonical backend changes,
and aligned/unaligned operator snapshots.

Flink task-harness tests additionally restore pending windows through aligned and unaligned
checkpoints on both backends. The unaligned cases replay an in-flight Arrow exchange frame and
verify exactly-once output after the restored watermark. Flink's state-repartitioning utility
also exercises 1→2 rescaling across 16 key groups, including canonical savepoints that switch
backends; duplicate emission order is compared against Flink. These are focused runtime checks,
not themselves evidence of Nexmark performance. Direct native tests additionally
cover migration, contract rejection, buffer ownership, cancellation, bounded workspace, and
ordered state operations.

**Retained implementation scope:** Partial implementation for bounded hash/adaptive/sort-merge/nested-loop joins and for
synchronous regular, multi-way, time-bounded, and temporal streaming joins.

Generated shared-region conformance tests also cover binary inner joins with a nullable
`TIMESTAMP(3)` range predicate evaluated by DataFusion. They exercise inclusive endpoints,
negative epochs, null and non-matching bounds, a 5,000-row fan-out crossing the bounded
expression chunks, all four RowKinds, complete registered metrics, backend-switch savepoints,
1-to-2-to-1 rescaling, incremental SST reuse, and actual aligned/unaligned channel replay.
The same matrix covers literal timestamp offsets using Flink-generated join conditions, including
overflow at the internal signed-millisecond limits. DataFusion evaluates those offsets with wrapping
integer arithmetic and zero-copy timestamp casts. A native regression checks both batched candidate
masks and a 50,003-candidate hot key: measured temporary allocation peaks fit the reserved Arrow
workspace, budget denial releases credit, and broker calls follow chunks rather than rows.
These tests support production admission for the bounded residual-comparison subset above.

Residual evaluation now also batches candidate pairs across consecutive incoming rows. A cache
holds at most 4,096 candidate pairs and 4,096 input descriptors, with an additional 8 MiB
coarse workspace limit for wide payloads. It uses one reservation for
masks/descriptors and one for the Arrow expression workspace. Incoming columns use zero-copy
slices when pair indices are contiguous and Arrow gathers otherwise. Opposite-side stored payloads
are decoded in a batch. A single larger fan-out uses the same row and byte bounds. The byte
limit is an internal vectorization quantum, not a deployment setting or a smaller accounting
allowance: the full per-pair reservation is unchanged. A single pair larger than the quantum is
attempted with its full reservation and fails recoverably if Flink cannot accommodate it.
When Flink denies a chunk reservation, the operator halves the candidate work before allocating
Arrow arrays and retries, down to one pair. This adapts to available budget without changing
per-pair accounting, materializing a failed chunk, or repeating a state transition. The ordinary
successful path does not add a budget query.
Generated wide-payload Flink tests compare all changelog transitions and registered metrics on
both backends; native tests cover cross-row chunks, hot keys, and oversized-pair admission.
The state transitions still consume these masks in original input order, including outer/semi/anti
association counts in retained implementations. State writes and output draining retain their
existing state batch boundaries, and the persisted format is unchanged. If predicate or output
admission is denied while an output prefix is ready, the stream emits that admitted prefix and
resumes its cursor on the next pull. An empty output halves its fan-out target down to two slots,
keeping a null-padding retraction adjacent to its joined row. These smaller Arrow outputs add no
state reads/writes or native-plan invocations. If the minimum transition cannot fit, execution
still fails recoverably. Generated wide-payload metric/changelog tests and native pressure tests
cover these continuations, including exact persisted state and one write per input batch.

The Q4 release profiles before this change attributed about 6–7% of process CPU samples to JVM
memory reservation callbacks. A native regression test reproduced 4,119 budget callbacks for
1,024 residual-filtered rows; the same workload now requires fewer than 64. This is callback-count
and parity evidence. The [Q4 release comparison](/StreamFusion/benchmarks/q4-rowdata/)
reports matched measurements and separate profiles on both backends.

## Cached CSV lookup joins

Synchronous inner equality lookups over Flink's legacy `CsvTableSource`, including Nexmark Q13,
are admitted through ordinary planner selection. The native lookup composes with adjacent Calc
stages in one Arrow/DataFusion tree, including a shared lookup with multiple native consumers.
The source description is serialized separately from the protobuf plan and opened only at task
open. No RowData-shaped intermediate operator or additional per-probe JNI crossing is introduced.

Equality keys must be matching boolean, signed integer, VARCHAR or VARBINARY fields; composite
keys are supported. Other sources, asynchronous lookup, retries, outer joins, constant keys,
custom shuffle, upsert materialization, embedded side projections/filters, and pre/residual join
filters retain precise whole-plan fallback. Ordinary Calc stages before or after the lookup remain
composable. A lookup cannot yet share a native region with keyed state: Flink initializes that
state before opening the snapshot. This restriction is checked before committing the selected
graph, including single-output trees, and does not affect Q13's Calc/lookup region.

The source boundary extracts Flink's configured legacy `CsvTableSource` reader through its public
`getDataStream` / `createInput` APIs. An isolated environment captures the reader description;
it executes no job and reads no file during planning or serialization. Task open creates a fresh
reader and lists all splits with the same argument as Flink's `CsvLookupFunction`. The
lookup binding drains this finite source before accepting probe records and reloads it when
the task recovers. It does not create Flink's additional Java hash-map cache.

The adapter writes source rows directly into independently owned Arrow batches under a
caller-supplied allocator. Returned batches remain valid after later reads and reader closure;
allocation/parse failures close the active file and release the current batch. Flink retains all
configured CSV parsing, including projected columns, comments, quoting, delimiters, headers,
lenient parsing and empty-column nulls. Its standard temporal conversion adapts the legacy
reader's `java.sql` objects while preserving declared logical types and decimal precision.

Six focused Java tests compare generated lookup values, order and binary-row bytes against the
actual Flink CSV lookup function, and cover serialization before the file exists, multiple files,
nullable/duplicate keys, varied batch sizes, temporal/decimal payloads, reopen, early close and
allocation/parse failure. These source-adapter checks supplement the runtime parity tests below.

The test-only DataFusion 55 lookup investigation verifies an inner `HashJoinExec` with a
single-consumption Arrow build stream and repeated probe invocations. Generated cases cover
nullable BIGINT/composite UTF-8 keys, dense and sparse keys, duplicate file order, all four
RowKinds, record timestamps and ordinals, empty input/cache, and projection composition. A
40,003-match key exercises bounded output batches. Build reservations reach the Flink memory
broker, remain stable across probes and release after success or memory denial.

This candidate is deliberately excluded from production builds. DataFusion registers fresh
metric descriptors on every `execute` call; a 32-invocation regression verifies linear retained
descriptor growth despite stable build-buffer reservations. Also, an unordered outer-join probe
can emit unmatched rows after matched rows, violating Flink's arrival order. These checks are native
algorithm/lifecycle evidence against an independent row oracle, not complete Flink/native changelog,
metric or recovery parity.

A retained native inner-lookup stage now uses DataFusion's unchanged `JoinHashMapU32`,
`update_hash`, bounded candidate probing and physical equality expressions, followed by Arrow
gather kernels. It builds the immutable hash table once, preserves duplicate file order, and
emits each batch's matches before requesting another input. Equality checks remove hash collisions
with SQL null semantics; supported key types are boolean, signed integers, UTF-8 and binary,
including composite keys. Stable physical-stage counters count logical probe and output records.
The probe streams do not construct DataFusion plans or retain new metric objects per invocation.

This is a documented Flink-lifetime deviation from Comet's use of `HashJoinExec`: repeated
execution grows retained metrics in DataFusion 55, while keeping that operator's stream alive
coalesces small results across Flink control boundaries. StreamFusion manages immutable cache
lifetime, bounded probe cursors and Arrow ownership; DataFusion still performs hashing, candidate
enumeration and equality computation. It does not copy or privately modify DataFusion's algorithms.
The hash-table API is public but intended mainly for DataFusion's internal use, so dependency
upgrades must rerun the lookup regressions.

Coarse reservations cover hash state, probe hashes, candidate indices, comparison work and output.
An output pull examines at most 1,024 candidates and reduces that limit when wide selected values
do not fit; an unrepresentable minimum batch fails recoverably. Shared snapshot buffers retain
their existing Arrow leases, and output leases survive downstream projection and task-plan closure.
Native tests cover every admitted key type, generated ordered changelogs and metadata, 2,000
invocations with constant cache credit, a separate heap-growth observation, live-stream delivery,
wide duplicate fan-out, cancellation, allocation denial and retry.

The version-3 physical-plan protocol now describes a unary `LookupJoin` with explicit probe,
snapshot and output schemas, equality-key positions and an inner-join mode. Its task resource is
bound by the original physical node identity; source configuration, process addresses and snapshot
payloads are excluded from the portable plan. Native construction rejects older protocols, missing
snapshot bindings, unsupported modes, wrong key types and mismatched schemas. Configuration and
schema retention use the same Flink memory pool as the cache and probe buffers.

Shared-context tests execute `Calc -> LookupJoin -> Calc` as one native tree across repeated
batches and watermark, checkpoint-preparation and end-input controls. They compare complete Arrow
payloads and record metadata against an independent arrival oracle, check each stage's logical I/O
counts, and verify stable cache credit and full release on closure. Probe columns are matched by
position, type and nullability so a preceding DataFusion projection's internal names do not prevent
composition. The immutable cache has no timers or separately checkpointed keyed state. These tests
verify native control traversal, not Flink checkpoint/restore or end-to-end metric parity.

A versioned task-open C Stream edge now connects this source to native lookup resources before
capability negotiation. Source descriptions stay in Java, and the original plan-node ID identifies
each finite stream. Native setup validates all identities before consuming any stream, drains the
snapshots before returning, and installs the cache bindings transactionally. Every consumed stream
is released once, including reader errors; Java closes unconsumed streams after setup failure.

The existing Arrow buffer registry retains producer release callbacks without charging Java-owned
payloads again. A single source batch stays zero-copy. Multiple chunks are consolidated once under
an admitted construction allowance for DataFusion's flat build-row addressing, matching its hash
join build model; this is task-open cache construction, not an intermediate batch handoff. Coarse
reservations also cover the retained chunk descriptors, hash table and physical configuration.

Generated Java boundary tests compare serialized changelogs with Flink's actual CSV lookup
function across source chunk sizes, duplicate/null keys, Unicode values and all four RowKinds.
They also check logical native-stage counters and record timestamps. A shared Flink managed-memory
fixture verifies that increasing retained Java payload grows the total charge only once, that
parse/budget failures return all credit, and that output remains valid after the cache closes.
Changing the file affects the next task open; an existing task retains its original snapshot.
Generated runtime tests additionally invoke Flink's unchanged lookup code generator,
`CsvLookupFunction` and `ProcessOperator`. They compare the complete serialized changelog,
duplicate order, SQL-null probes, record timestamps, watermark/idle controls, logical-record
counters, and complete registered operator metric names/types and deterministic values. Rate
meters retain Flink's implementation and logical counters rather than matching wall-clock rates.
Calc/lookup/Calc and shared-output tests verify independent stage counts and Flink's Calc timestamp
clearing. Factory serialization succeeds before the source exists; cancellation returns cache
credit. Aligned and unaligned operator snapshots contain no lookup state, matching Flink, and
restore reopens the original source. Channel state and redistribution remain Flink-owned; the
lookup has no keyed state to migrate or rescale. These stateless tests are not RocksDB state
performance evidence.

The original Q13 SQL passes ordinary EXPLAIN admission and blackhole execution with both HashMap
and RocksDB configured, with positive native batch counters and exactly the expected bid count.
The [Q13 release comparison](/StreamFusion/benchmarks/q13-rowdata/) includes matched measurements
and mixed JVM/native profiles. At ten million events, median throughput is 1.113× Flink with
HashMap and 1.149× with RocksDB configured; one-million-event runs remain slower. Lookup itself
is about 3% of process CPU in the longer profiles, with larger costs in the shared source and
RowData/Arrow boundaries. These results do not establish a performance ceiling.

## SQL example

```sql
SELECT b.bidder, b.price, p.name
FROM bid AS b
JOIN person AS p ON b.bidder = p.id;
```

## Acceleration and fallback

The retained native runtime implements Flink's synchronous regular streaming `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` joins when both sides use non-unique multiset state; production
selection remains subject to the architecture-admission restriction above. A join may
combine its non-empty equi key with a generated residual predicate; the residual is evaluated over
the concatenated left/right row with SQL three-valued Boolean semantics, including treating `NULL`
as non-matching.
Stored rows may use any Arrow-representable Flink scalar or nested logical type, while join keys
may use every such type that Flink accepts for SQL equality. The native operator accepts the
complete insert/update-before/update-after/delete changelog, retains
duplicates, applies Flink's per-key null filtering, and reproduces Flink's null-padding and
association-count transitions.

The two-input streaming bridge executes a persistent `RegularJoinExec` and downstream native fragments
inside one reusable native plan tree. Left/right ports are explicit protobuf children; native
stages share Arrow arrays directly and only the outer output stream transfers accounting to Java.
The join's logical output counters describe its own output before downstream stages, which have
separate stage counts. Flink-specific code owns keyed multiset updates, retraction counts and
per-arrival transitions: DataFusion's symmetric hash join consumes append-only Arrow rows and
has no Flink RowKind/retraction or key-group snapshot contract. DataFusion evaluates residual
predicates and Arrow kernels gather output; equality-only keyed candidates require no residual
computation. Generated tests against both Flink join functions verify all four RowKinds and
control boundaries for the admitted inner subset. Selected regular joins use the same fragment/state-capability interface
as deduplication, with one generic keyed-region runtime and no special lifecycle-owner fusion hook.
The region retains planned exchange frames and decodes them once at its input edge.
Metric-owner lookup follows the protobuf tree rather
than assuming a Calc tail. Protocol-v2 Calc fragments and the common native envelope preserve
RowKind through downstream projections and expansions. This foundation still requires complete
large-owner/metric admission. Direct topology tests combine join, Expand, Calc and deduplication in one
region, but arbitrary internal key transformations, bounded families, timers and full multi-stateful
SQL admission remain unfinished.

Streaming output reservations now follow shared Arrow buffer owners rather than the producer's
next pull. Retained projections, nested children, and slices stay charged after the producer closes;
no payload copy is introduced. The common edge recognizes registered owners, including imported
producer-owned buffers, and does not reserve their payload again. Large kernel workspaces still
require admission before allocation.

The common state binder copies only the join's configuration, not either physical child subtree.
Construction reserves decoded-plan memory before protobuf decoding and uses shared type-shape
admission before allocating Arrow schemas and row codecs. Wide/nested-schema allocation tests and
denied-construction cleanup tests cover those allowances independently of batch scratch. Residual
expression allocation and the remaining lifetime caches still require the complete admission audit.
Canonical paged-state restore drops its validation-only decoded pages and allowance before invoking
the backend restore. Both backends have a constrained-budget regression that allows either phase
but not both workspaces simultaneously; validation still finishes before any state is replaced.

The retained streaming join admits manifest/page lookup descriptors and dirty-state mutations
once per incoming batch. Output buffer allowances grow geometrically with a 64 KiB minimum
and remain held until that output chunk is emitted, avoiding a JVM reservation for each row
or touched key. Admission still precedes allocation, and budget failures require recovery.
A 1,024-key regression checks bounded broker-call counts alongside changelog and cleanup tests.

The retained two-input streaming runtime emits bounded Arrow C Stream batches, capped at 4,096
rows with a smaller row target for wide payloads. It does not first collect the complete fan-out.
Residual predicates use bounded vectorized candidate chunks, while equality-only/null-rejected
keys need no match bitmap. Input and historical-state memory remains charged until the input batch
is fully consumed. State uses stable per-side row IDs and 64-row pages. Batch admission fetches all
manifests in one lookup, then any external pages in a second lookup; the single end-of-batch
write contains only changed pages and changed manifest metadata. Keys with at most 64 total rows
and an encoded size of at most 8 KiB keep their directory and payloads in one compact backend
entry. This removes a second entry and lookup for sparse keys; larger keys keep independently
writable pages. Growth, shrinkage and deletion switch representations in the same atomic write. Retracting an early row does not
shift later pages, and association-count changes rewrite only their affected pages. Historical rows
are still decoded for touched keys, so this is a write-amplification fix, not a claim of constant
read or working-set cost for arbitrarily large keys. Checkpoints cannot observe a partially drained
batch. Cancellation or failure
requires task recovery; the stream safely retains native ownership if its Java handle is released.
After the final row has produced owned state mutations, the join drops decoded state, lookup
indices, input encodings and exhausted predicate masks before admitting backend growth. Mutation
buffers retain one coarse allowance, and final output payload Arcs retain their separate output
allowance. A constrained-budget regression verifies this handoff and probes all retained payloads
from the opposite input. This preserves one atomic write per incoming batch and the existing
failure/recovery boundary.
Logical Flink I/O counters count records across all output chunks; StreamFusion's processed-batch
diagnostic counts each input once, not each output pull.

Streaming join input admission uses the logical byte spans of flat Arrow columns when sizing
row-encoding workspace. Multiple columns or slices can share one IPC allocation; the original
buffer retains its producer's reservation and is not counted again for every column. The coarse
workspace still covers row-format padding, equality keys and lookup metadata. Nested, dictionary
and view columns retain their existing conservative estimate. A shared-IPC regression processes
2,048 rows with 512-byte payloads, including a non-zero-offset slice, within a 16 MiB share and
checks allocation peaks and joined payloads; the prior estimate requested over 18 MiB at input
admission alone. Wide nullable flat-schema tests verify encoding/decoding and workspace bounds.
The lookup-directory allowance covers its largest overlapping phase: the temporary unique-key
table is dropped before constructing staged state. A 16,384-row test with one, 512 and 16,384
distinct keys fits input admission within 12 MiB and verifies allocation peaks and cancellation.
The previous allowance requested over 17 MiB even for the single-key input.

Paged decoding admits payload bytes and coarse row-vector/Arc headroom once for the complete
read batch, using the page headers. Original and updated state share payload Arcs, so wide rows
are not charged as eight hypothetical decoded copies. Backend read buffers retain their own
reservation. A constrained 10 MiB regression loads 2,048 historical rows at empty, narrow and
1 KiB payload widths, verifies shared payload ownership and decoded equality, and checks observed
allocation peaks against the reservation. Arbitrarily large touched keys can still exceed the
allowance and require recovery; the change does not bypass Flink's budget or add per-row I/O.

Canonical SFS1 snapshots carry versioned `SFJM` manifests, `SFJP` pages and `SFJC` v1 compact
entries, identical across native memory and RocksDB. Existing paged snapshots remain readable;
small keys adopt the compact form when next written. Restoring legacy whole-key `SFRJ` v1/v2
snapshots migrates them to the current layout. A runtime predating `SFJC` cannot restore new compact
entries. Tests cover both-backend restore, sparse stable row IDs, both size thresholds, conversion
in either direction, complete deletion and malformed/truncated records. A 100,000-key regression
with 160-byte payloads fits a 36 MiB in-memory share and probes the retained rows; the previous
separate-entry representation exhausts that same share.
Compact-record decoding sizes its workspace from embedded page headers: payload bytes and coarse
row-vector/Arc headroom. It does not apply the external-directory multiplier to embedded payloads.
The first decoded page moves directly into its state vector. An 8 MiB regression loads 2,048
compact keys with 512-byte payloads in one state read, verifies original/current payload sharing,
and releases all credit; the previous directory estimate requested more than 9 MiB for decoding.
Mutation staging likewise counts only records present in the old and new physical layouts:
one root for a non-empty compact entry, with external-page metadata only for paged entries.
A 16,384-key batch now fits a 20 MiB share; its allocation peak is covered by coarse reservations,
and retained payloads are probed after flushing. The previous estimate added almost 25 MiB for
mutation metadata alone by counting absent roots and external pages. Layout transitions and
canonical bytes remain covered by the same both-backend parity and recovery fixtures.
Restore rejects missing, duplicate, orphan, and malformed page records before changing backend state.
Physical RocksDB checkpoints retain the paged layout and the existing incremental checkpoint protocol.
The `StreamFusion.stateReadBatches` diagnostic counts actual backend lookups: one for a batch of
new or compact keys, two when external historical pages exist. `stateWriteBatches` counts non-empty backend write batches, so a
missing-row retraction that changes no state need not increment it.

Flink `BatchExecHashJoin`, `BatchExecAdaptiveJoin`, and `BatchExecSortMergeJoin` equality joins use
the same native two-sided counted state with terminal output while retaining distinct physical-node
identities for planning and metrics. Terminal matching now uses DataFusion `HashJoinExec`, including
its outer/semi/anti algorithms and residual `JoinFilter`, instead of a handwritten pair cursor. `BatchExecNestedLoopJoin` uses one singleton key group and
evaluates its complete predicate as a vectorized residual condition. Bounded `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` results are emitted as insert-only Arrow batches after both
inputs end. Duplicate rows, null join semantics, residual predicates, and all four input row kinds
are supported. Terminal output is capped at 16,384 rows per Arrow batch, including for one hot
cross-product key.

The retained `StreamExecIntervalJoin` implementation supports constant row-time or processing-time
bounds and `INNER`, `LEFT`, `RIGHT`, or `FULL` joins. Both sides retain timestamp-ordered
multisets in native keyed state. Native event-time or processing-time cleanup timers delay outer
null rows until no future match can arrive, and retractions reverse both joined and previously
emitted outer rows using Flink-compatible association counts. Keys and stored rows have the same
complete Arrow-representable scalar and nested type coverage as regular joins.

The retained `StreamExecTemporalJoin` implementation supports event-time `INNER` and `LEFT` temporal
table joins and processing-time `INNER` temporal table-function joins. Event-time probes retain
their left changelog until the two-input watermark makes the version lookup final; processing-time
probes read the current right version and honor Flink's idle-state retention interval. The native
operator accepts all four row kinds on either input, applies Flink null-key filtering, and supports
generated residual join conditions. A failed residual predicate drops an inner result or emits a
null-padded right row for a left join. Keys and stored rows support every Arrow-representable Flink
scalar and nested logical type.

The retained `StreamExecMultiJoin` implementation supports plans where all join predicates are represented by its
attribute-based equi-join map and all inputs share a non-empty partition key. The native continuation-based
operator supports Flink's `INNER` and `LEFT` chain shapes, duplicate multiset rows, all four row
kinds, SQL-null join semantics, and the null-padding retraction/insertion transitions of chained
left joins. Stored payloads and predicate fields accept every Arrow-representable Flink scalar and
nested logical type.

A two-input `StreamExecMultiJoin` with a common equi key is lowered to the regular native join.
When its entire condition is covered by the common equality keys, lowering omits the redundant
residual predicate: keyed state lookup already enforces those equalities and null filtering. This
avoids decoding candidate payloads and evaluating an Arrow comparison for each probe. Conditions
with additional comparisons retain the complete DataFusion residual, including equalities outside
the partition-key map. This applies to binary equi-joins generally, including Flink's physical forms
for Nexmark q3, q4 and q9; it does not change production admission. Three-or-more-input multi-joins use the multi-way cursor and still
require every predicate to be represented by the attribute map.

Multi-way state now uses a per-key directory and independently persisted 256-row pages. Stable
row slots preserve insertion order; only dirty payload pages and their directory are written at
batch completion. Required directories and payload pages are read in batches before computation.
The output cursor retains join-depth continuation state and emits at most 4,096 rows or roughly
1 MiB of encoded payload per pull (one oversized row requires sufficient memory credit). It never
collects the complete Cartesian product. Output leases survive the processor if a consumer keeps
a batch. Checkpoint/restore requires a healthy, fully drained stream, and partial computation
failure requires recovery. The old single-batch harness rejects fan-out needing multiple pulls.

This paged format replaces the development-only whole-key multi-join format; old multi-way
snapshots are not a supported migration boundary. Production selection remains disabled until
the cursor is connected to the common fused execution and Flink metric/checkpoint lifecycle.


The containing plan falls back to Flink with an EXPLAIN reason when a streaming regular join has no
usable equi key, state TTL, mini-batch execution, asynchronous state, changelog-state wrapping, or a
planner-provided unique/upsert key. Interval joins additionally fall back for a residual non-equi
condition, non-constant bounds, semi/anti join modes, mini-batching, asynchronous state, or
changelog-state wrapping. Temporal joins fall back for right/full/semi/anti modes, asynchronous
state, or changelog-state wrapping; Flink itself rejects processing-time temporal table joins, and
only its temporal table-function form is accepted there. Lookup joins outside the cached CSV
subset above remain Flink-owned. A
bounded nested-loop scalar-subquery join also remains on Flink because its single-row cardinality
failure contract is not yet native. These are explicit unimplemented shapes, not approximations.
Multi-way joins additionally fall back for residual predicates outside the attribute map,
planner-provided unique/upsert keys, non-zero state TTL, mini-batching, asynchronous state, or
changelog-state wrapping.

## Implementation

The planner replaces an eligible streaming or bounded join with a distinct StreamFusion exec node and sends
a versioned protobuf join contract to Rust. Each input crosses a native Arrow exchange edge; the
join itself receives Arrow batches and returns Arrow batches without a Java RowData payload loop.
A regular streaming join followed by one or more eligible Calc nodes is lowered as one
persistent native join handle with a reusable DataFusion Calc tail. Those stages exchange the
join's Arrow `RecordBatch` directly in Rust, so the fused edge adds neither a Java operator nor an
additional data-plane JNI round trip. The same path is used by bounded hash and nested-loop joins.
This retained implementation is not yet a single DataFusion ExecutionPlan containing both the
persistent join and its Calc stages, and does not close the general fusion admission requirement.
Other stateful operator families currently retain their own native handles as well.

Rust stores an ordered multiset for both input sides under a Flink-compatible key group. One input
batch loads all touched manifests and then their referenced pages in at most two distinct batched
reads, followed by one atomic write batch containing only changed pages and metadata. The same opaque state
contract runs on managed native memory or direct native RocksDB. Canonical key-group snapshots move
between those backends and across parallelism, while ordinary RocksDB checkpoints use the shared
incremental-SST lifecycle. Aligned and unaligned checkpoints preserve join state and the two input
watermark frontiers. Regular joins do not register timers. Interval joins use the shared native
timer service and materialize dirty timer groups into canonical keyed state at snapshot boundaries,
avoiding repeated whole-group timer serialization during normal batch processing. Both variants
coalesce and forward watermarks using Flink's two-input rule.

The bounded DataFusion join receives each Flink equality-key partition after the canonical
multiset has applied input retractions. A nullable synthetic key preserves the partition's
mixed null-safe/null-filtered equality decision, and DataFusion evaluates any remaining SQL
predicate over Arrow columns. This retains Flink's state/checkpoint ownership while delegating
matching to DataFusion. Its hash table uses the Flink-backed DataFusion memory pool; Arrow input
and bounded output identity buffers have coarse host reservations. One native runtime is reused
across key partitions. Generated direct-native tests compare all six join kinds to Flink SQL on
memory and RocksDB, including null keys, duplicates and a retraction before terminal output.
The shared-plan/configuration/metric admission gates are unchanged.

Bounded hash/adaptive/sort-merge joins partition both sides by the planned Flink equality key. The
native sort-merge replacement uses DataFusion hash-join computation over the already keyed
terminal state rather than sorting both inputs: SQL does not promise join output order, and a downstream Flink sort still
enforces any explicit `ORDER BY`. Its output changelog and null/residual semantics remain identical.
Bounded nested-loop joins discard Flink's broadcast/ANY exchange wrapper and install a native singleton
exchange because the complete cross-product condition is evaluated in Rust. Neither path builds a
second Java hash table or sorter. Both memory and direct RocksDB state perform the same manifest/page
batched reads and dirty-page write batch per incoming Arrow frame. Aligned, unaligned, and canonical
cross-backend restoration use the same SFS1 key-group bytes as streaming regular joins, and
ordinary RocksDB checkpoints retain incremental SST reuse.

The exchange frame is decoded directly by the native bounded join, so it does not become an Arrow
Java batch merely to cross JNI again. Primitive keys are encoded and assigned to Flink key groups
in Rust. For ROW and other Flink-equality-comparable complex keys, the exchange retains its already
computed opaque Flink `BinaryRowData` sidecar through this one native consumer edge; the same
transport also supports ARRAY, MAP, and MULTISET keys when used through a lower-level Flink runtime
contract that admits them. Rust consumes the bytes for equality and key-group ownership, and the
sidecar is never exposed in SQL output or by an ordinary exchange reader.
Aligned exchange can therefore continue coalescing key groups per destination, while unaligned
exchange can retain one frame per key group without changing the state format.

Regular-join residual predicates are encoded in the same versioned protobuf expression contract as
Calc and lowered to a DataFusion physical expression. Streaming regular joins evaluate candidate
pairs in Arrow chunks bounded by 4,096 rows and an 8 MiB workspace quantum, retaining the match bitmap
while output drains. Equality-only and null-rejected keys use constant masks without allocating a
candidate bitmap. Evaluation retains Flink's per-record state-transition order. Association counts and
outer/semi/anti transitions count only accepted candidates. Predicate scratch, state, and exported
Arrow output are all charged to the operator's Flink managed-memory reservation.
ROW, ARRAY, MAP, and MULTISET values are accepted as opaque equality keys and stored payloads, but
comparisons over those nested values inside a residual expression remain Flink-owned. The planner
rejects the complete plan with that precise reason rather than approximating nested comparison
semantics in Rust.
The regular and interval join transformations each request a stateful relative weight of eight
from Flink's existing `OPERATOR` pool; this is a share of the configured task memory, not a separate
StreamFusion memory setting.

Temporal joins use that same backend-neutral keyed-state and timer interface. Each incoming Arrow
batch performs one distinct batched state read and one atomic batched write. Right-side event-time
versions and pending left probes are encoded as opaque Arrow rows per Flink key group; the native
timer service releases and cleans them at the combined watermark. Processing-time state keeps the
current version plus its cleanup deadline. Canonical key-group savepoints restore interchangeably
between memory and RocksDB and across parallelism; aligned and unaligned checkpoints retain state,
timers, and both watermark frontiers, while ordinary RocksDB checkpoints reuse unchanged SSTs.
Residual conditions are evaluated by Flink's generated condition over Arrow-backed row views. When
all candidates pass, the bridge transfers the native output buffers without copying them; mixed
pass/fail results copy only the selected or null-padded output required by the predicate.

Multi-way joins reuse the same native keyed-state interface through a V2 multiple-input operator.
Each incoming Arrow batch performs one distinct backend multi-get and one atomic write batch for
all touched common keys. Per-input ordered multisets are encoded as opaque Arrow rows, while equi
predicate fields use opaque Flink binary-key sidecars so nested and non-native hash types never
cross JNI per row. The canonical key-group representation restores across parallelism and between
managed memory and direct RocksDB; RocksDB uses the shared incremental-SST checkpoint path.
The operator requests its scratch allowance from Flink's ordinary `OPERATOR` managed-memory pool
in proportion to its input count. State, decode scratch, recursive candidate traversal, and exported
Arrow output remain covered by that reservation; candidate traversal borrows stored rows rather
than cloning them, and native output reuses the columns produced by Arrow row decoding instead of
performing a second identity gather. Source-edge Arrow allowances scale with the physical nested
vector tree, so complete logical-type payloads remain admitted without creating a separate memory
budget.

Deterministic native transition tests cover every join type, residual acceptance and rejection,
duplicates, retractions, null keys, residual restore after rescaling, and canonical residual-state
migration from memory to RocksDB with batched I/O. Bounded coverage additionally checks terminal
insert-only results for all six join modes, pre-terminal retractions, null filtering, vectorized
residuals, hot-key output chunking, memory accounting, key-group rescaling, every checkpoint mode,
all four memory/RocksDB restore combinations, and direct exchange ingestion with an opaque ARRAY
key. Generated bounded SQL parity tests cover all six join modes, duplicate keys, nested payloads,
outer null padding, and a residual condition.
Interval coverage additionally exercises every
supported logical type as both a key and stored value, pending event-time and processing-time
timers, aligned and unaligned checkpoints, canonical cross-backend savepoints, and incremental
RocksDB restore. The generated SQL parity suite uses INNER and SEMI regular joins because their
result changelog is invariant to the test harness's independent two-input source scheduling;
outer and anti transition ordering is checked with controlled native input order. A deterministic
constant-bound interval-join workload compares the complete Flink and StreamFusion changelogs on
both state backends. Temporal tests cover both time modes, all row kinds, residual inner/left
semantics, every supported key and state type, managed-memory admission, metric parity, canonical
memory/RocksDB restoration, rescaling, aligned and unaligned checkpoints, and incremental RocksDB
SST reuse. Its SQL parity test compares the complete changelog against Flink on both backends.
Multi-way join tests cover three-input duplicate inner joins, ordered chained-left null-padding
transitions, missing retractions, SQL-null predicates, key-group rescaling, and canonical
state migration in all four memory/RocksDB source and target combinations. Java operator-harness
coverage checks the complete logical-I/O, row-kind, state-I/O, checkpoint, failure, watermark,
timer, and backend metric surface; aligned and unaligned restore on both backends; incremental
RocksDB SST reuse; and one-to-two-subtask key-group redistribution. Generated SQL tests require an
accelerated EXPLAIN and non-zero native batch count for both inner and left shapes; the left SQL
fixture uses disjoint right inputs so its complete changelog is deterministic despite independent
bounded-source scheduling.

Official Nexmark q4 and q9 integration cases compare the final keyed table against Flink on both
state backends and require accelerated EXPLAIN output with positive shared native-plan counters.
Standalone join/aggregate/Top-N invocation counts are not acceleration evidence for a fused plan.
Independently scheduled join inputs can produce different intermediate aggregation/rank transitions,
including across repeated unmodified Flink runs; fixed-arrival harnesses separately compare complete
changelog bytes. The result collector retains separate raw sorted and
arrival-order changelog hashes; its primary-key-aware materialization applies upserts before sorting
so a legal `UPDATE_AFTER` is not miscounted as another table row.

See the [Flink 2.3 Joins documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/).
