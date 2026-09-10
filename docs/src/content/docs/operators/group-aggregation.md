---
title: Group aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Group aggregation.
sidebar:
  order: 6
---

**Current status:** Partial. Ordinary planning admits synchronous keyed `StreamExecGroupAggregate`
with BIGINT `COUNT` (including DISTINCT), and non-DISTINCT BIGINT `SUM`, `SUM0`, `MIN`, `MAX`,
and `AVG`. Append-only, non-DISTINCT VARCHAR `MIN`/`MAX` are also admitted when the aggregate
input directly consumes an original Flink HASH exchange. Grouping keys must be BIGINT, INTEGER,
or VARCHAR. Both in-memory and supported default RocksDB state use the common native execution tree. Existing semantic checks still
reject unsupported TTL, async state, metrics, and backend configurations.

Mini-batch, singleton/global, SELECT DISTINCT, other argument/result/key types, and other physical
aggregate families retain whole-plan fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/).
The broader native paths below remain development implementations, not production coverage.

VARCHAR extrema use DataFusion's UTF-8 byte ordering. Flink's `BinaryStringData.compareTo` can
instead use UTF-16 ordering when both operands cache Java strings; those orders differ across
some supplementary Unicode characters. Flink's HASH exchange uses a non-chainable keyed
partitioner and RowData serialization, so every incoming string is binary-backed and preserves
DataFusion's order. Admission requires that direct boundary and does not look through a subsequent
Calc that might recreate Java-backed strings. Retractable string extrema, DISTINCT variants and
unproven input boundaries retain precise whole-plan fallback. There is no upstream runtime patch,
extra production serialization, character restriction or new configuration option.

Generated tests pass identical logical inputs through the original exchange's RowData wire
representation into Flink's SQL-generated handler and through Arrow into the native region.
They compare every changelog byte, record envelope, watermark and registered metric for nulls,
empty strings, embedded NULs, trailing spaces, combining characters, BMP/supplementary ordering,
long values, nullable FILTERs, composite keys and mixed BIGINT DISTINCT counts. Recovery tests
cover canonical/backend-switch restore, full/incremental aligned/unaligned checkpoints, actual
channel replay and 1-to-2-to-1 rescaling through all 16 test key groups. A separate comparator
test retains the Java-backed ordering difference and verifies the planner boundary guard.
The upstream Flink `AggregateITCase.testBigDataOfMinMaxWithBinaryString` SQL/input case
and a generated Unicode SQL case also compare complete changelogs through ordinary planning
on both backends. The original Nexmark Q16 SQL reaches the unmodified blackhole sink with
positive native batch counters on both backends. The RowData benchmark catalog includes the
unchanged Q16 SELECT and its original result schema; its release performance comparison is pending.

Large incoming and historical string values remain subject to coarse batch admission. A tiny
input batch may repeat a large retained maximum in every UPDATE_BEFORE/UPDATE_AFTER record;
credit for those event copies and Arrow output is admitted before state mutation. Native tests
exercise denied large incoming values and repeated 64 KiB historical extrema on both backends,
verify unchanged state after denial, and retain snapshot accounting until its last owner releases it.
Evaluated DataFusion BIGINT and VARCHAR scalars transfer directly into the Flink state adapter,
without creating and reading a one-element Arrow array for each changelog transition. VARCHAR
transfers its owned bytes; the accumulator computation and every required intermediate result
remain unchanged. Other scalar types retain their existing conversion. The existing canonical
accumulator encoding, Flink budgets and metric definitions are unchanged.

**Retained implementation scope:** Partial implementation for timer-free keyed and global streaming aggregates and
bounded hash aggregates, including grouping sets, `ROLLUP`, and `CUBE` in both runtime modes.

The synchronous, non-mini-batch streaming path now binds its state into the shared native
execution context and uses the common unary DataFusion adapter. Adjacent Calc stages exchange
Arrow batches directly; native RowKind and input-ordinal metadata preserve retractions and record
timestamps. Both memory and direct RocksDB bindings use the same node-addressed canonical
snapshot/restore interface. Output admission follows its Arrow buffers through retained slices
and producer close, without transferring to Java between native stages. Plan/codecs and cached
schemas have independent lifetime reservations, and failed schema preparation is transactional.

Aggregate computation now delegates compatible calls to DataFusion accumulators. Bounded
aggregation and local/append-only mini-batches call `update_batch` on Arrow group selections;
streaming aggregation retains a DataFusion accumulator per loaded group during
an incoming batch and evaluates every required intermediate changelog result. Expressions and
input casts are prepared once per batch, not rebuilt per row or group. This includes COUNT,
integer SUM/SUM0, integer AVG's sum component, and append-only MIN/MAX over non-floating types.
Flink-specific adapters retain integer result widths, AVG division, null/SUM0 behavior,
canonical state encoding, key ownership, bundle boundaries, and logical-record metrics.

The retained `COUNT(DISTINCT argument)` row and append-run paths also use DataFusion COUNT.
Flink's signed membership map produces a marker only for a first accumulation or last retraction;
DataFusion updates or retracts the count from those markers. Nullable filters and arguments still
follow Flink, including an unmatched retraction and its later cancellation. Streaming execution
prepares one constant Arrow marker per call and retains the count accumulator per loaded group
for the incoming batch. Append runs build one nullable marker vector under the existing batch
workspace allowance. This adds no per-row backend access, JNI crossing, or input-payload gather.
Compact grouped vectors cannot consume these kernels directly because they lack the membership
map. DISTINCT SUM/AVG and canonical partial merging retain their existing semantic adapters.

Synchronous COUNT(DISTINCT) now stores memberships separately from the small group accumulator
header. Calls on the same input argument share one member entry containing independent
membership for their FILTERs, following Flink's DISTINCT data-view grouping. Retractable groups
store signed counts; new append-only groups store presence bits. Each incoming batch
loads its group headers and then all required member keys in a batched lookup. The existing
DataFusion accumulators compute each observable transition from those loaded counts. One atomic
backend batch writes changed member entries and group headers after computation; there are no
per-row backend calls or extra JVM/native crossings. Repeated membership values outside the
incoming batch are neither decoded nor rewritten.

Member keys use Arrow row encoding behind a versioned, length-framed partition prefix and
argument identity anchored to its first aggregate call, so physical input-column reordering
does not change membership identity. Their key group remains the hash of the original Flink BinaryRow, independent
of Arrow ordering bytes. This layout applies to synchronous raw-input plans whose DISTINCT
calls are all COUNT over supported non-floating scalar arguments: Boolean, signed integers,
VARCHAR, decimal, date, time and timestamp. Production admission for DISTINCT remains the BIGINT subset
described above. Floating-point DISTINCT, DISTINCT SUM/AVG, mini-batches, partial merging and
bounded final aggregation retain their existing inline state paths and admission conditions.

The persisted group header is `SFGD` version 1 for counted membership or version 2 for
append-only presence, wrapping a version-6 accumulator record with external membership maps
omitted. Member vectors use matching `SFDV` versions: version 1 stores signed 64-bit counts,
while version 2 stores one bit per shared FILTER after the call-count prefix. Unused bitmap
bits must be zero. Existing inline
DISTINCT versions 4, 5 and 6 migrate on the next update to each group, in the same atomic write
as that update. Migration preserves signed counts and nullable FILTER behavior. Unknown versions
and malformed count vectors are rejected. Canonical key-group snapshots include both headers and
members, so Flink continues to own checkpoints, recovery, channel replay and rescaling.

Append-only selection comes from the planner's input-changelog contract, not SQL text or a
benchmark setting. Existing inline and version-1 groups retain counted membership after an
upgrade, including every signed multiplicity; only newly created append-only groups use
presence. No historical scan or eager conversion is required. Presence state cannot restore
into a retractable plan: canonical and physical-file restore reject that mismatch before the
operator can process input. This format compatibility does not promise arbitrary SQL plan
changes across a savepoint.

Within a batch, the existing DataFusion COUNT kernels still compute every required changelog
transition. Temporary membership counts are normalized to presence before dirty-state comparison.
A duplicate-only batch therefore updates its group header without rewriting unchanged member
entries. The compact format changes retained state and write volume, not Arrow handoff,
metric definitions, memory budgets or production admission. With two shared FILTERs, a native
test verifies 15 fewer persisted bytes per member on both backends and no member writes for a
128-row duplicate batch. Generated tests use Flink's actual insert-only DISTINCT handler to
compare full changelog bytes, envelopes and metrics, plus canonical/backend-switch restore,
full/incremental aligned/unaligned checkpoints, channel replay and 1-to-2-to-1 rescaling.

Group deletion clears every persisted member, including unmatched negative counts, before a
recreated group becomes visible. Row-count transitions identify such deletions at the start of
the batch, when cleanup keys are loaded in bounded pages. RocksDB seeks to the group's prefix;
the HashMap backend scans its key directory and pages only matching entries. Unrelated inline
values do not consume that prefix's page allowance. Cleanup keys and one-time migration
workspace remain subject to Flink's allowance and may still exhaust it for a sufficiently large
deletion or old inline value. Ordinary updates reserve input-sized membership workspace rather
than the entire historical set. Retained backend entries, Arrow key buffers and dirty mutations
remain charged through the existing managed-memory model, with no new runtime settings.

Native tests build 10,000 members and verify that a one-row update reads and writes fewer than
256 state bytes with two batched reads, one write and no scan. They cover denied admission with
unchanged snapshots, shared filters, signed counts, deletion/recreation, Arrow key order and
framing, and migration plus restore across both backends. A controlled heap test also checks the
loaded maps when fifteen shared filters reject the batch but retain historical counts. These checks establish
bounded normal access; release throughput and larger-state capacity require separate measurement.

Generated common-runtime tests compare filtered and unfiltered BIGINT DISTINCT counts with
Flink's actual SQL-generated aggregate handler on both state backends, including all RowKinds,
duplicate and last-value transitions, null/Unicode keys, null arguments/filters, record envelopes,
watermarks, and registered metrics. Native tests additionally compare serialized state after
every transition, checkpoint round trips, sliced/noncontiguous inputs, and mixed append/retract
runs for BIGINT and VARCHAR arguments. The existing state encoding and reservations are unchanged.
Synchronous group deletion clears the staged accumulator, signed membership maps and cached
DataFusion kernels immediately when the input row count reaches zero. A later insertion in the
same Arrow batch starts a fresh group, and orphan retractions remain ignored. Generated tests
compare Flink's complete changelog, record envelopes and metric surface across batch sizes
1, 3, 7 and 64 on both backends, including a deletion that leaves unmatched membership counts.
Native tests also verify that batching does not change the resulting canonical checkpoint bytes.
Backend reads and writes still occur at batch boundaries; this cleanup adds no per-row state I/O.
Recovery tests additionally snapshot the original Flink handler and native region, restore
canonical state across both backends, and restore aligned/unaligned checkpoints on each backend.
They retain duplicate and negative memberships across the checkpoint, then compare every per-key
changelog byte while canceling unmatched retractions and deleting the final members. A 1→2→1
rescaling matrix routes actual Arrow exchange frames through all 16 test key groups and Flink's
state repartitioning. A two-channel mailbox test captures an in-flight duplicate with a different
filter result and replays it through Flink's channel-state reader exactly once. Managed memory
returns to zero after each native harness closes. These proofs cover BIGINT DISTINCT counts.
Ordinary SQL planning now admits that subset, including nullable FILTER predicates and multiple
distinct calls alongside ordinary BIGINT aggregates. Generated SQL tests compare the complete
Flink-serialized changelog multiset for three seeds on each backend, with all four RowKinds,
duplicate values, nulls and independent filters. DISTINCT SUM/AVG, non-BIGINT arguments,
global aggregation and mini-batching retain precise whole-plan fallback. The
[Q15 release comparison](/StreamFusion/benchmarks/q15-rowdata/) records measured performance,
separate profiles and the remaining large in-memory capacity limit.

Custom arithmetic remains for floating sums/averages (vectorized reassociation changes bits),
decimal arithmetic (Flink overflow can poison the accumulator), DISTINCT sums/averages, and
arbitrarily retractable extrema. DataFusion's distinct sets and ordered sliding extrema do not
represent the same signed/retractable state; DISTINCT membership remains a Flink-specific state adapter.
Mixed-changelog bundles use the same DataFusion accumulators while retaining their
ordered state updates and resetting the temporary cache at each Flink bundle boundary. These are specific semantic exceptions; wrapping custom computation in
an `ExecutionPlan` is not itself DataFusion compute reuse. Generated direct-native tests compare
bounded results to unmodified Flink SQL on both state backends, and the existing generated
streaming suites compare every changelog record. Production admission is limited to the synchronous keyed BIGINT and VARCHAR extrema subsets described above.

Planned schemas and row codecs now have a separate shape-based admission before Arrow type
lowering, including recursive codec construction's temporary null arrays. Compact protobuf byte
length alone underestimated these allocations: a controlled 64-field nested-key constructor
retained 45,317 bytes against 27,072 bytes of total credit before this fix. Construction credit now
shrinks to a conservative retained schema/call/codec estimate after initialization. Same-thread
Rust allocator tests cover 1/64/512-field keys, additional row/array nesting, and nested `COUNT`
payloads; they check decode admission separately, peak/live coverage, allocation-free sizing,
temporary-credit release, and cleanup when Flink denies schema construction. This is not yet a
complete shared-plan or C++/cross-thread allocation profile, and does not expand the supported production subset.

A generated SQL integration matrix now exercises 22 grouping-key families and nullable `COUNT`
payloads through the common native region, on memory and RocksDB with mini-batching disabled and
enabled. It compares the complete byte-encoded changelog multiset against Flink, using all four
RowKinds and independently changing each key field. This includes arrays, maps, multisets, and rows:
the shared native BinaryRow key codec now encodes their nested Arrow values into caller-owned
scratch storage. Separate Flink serializer fixtures check exact nested key bytes; sliced-array tests
check offsets and scratch reuse. The SQL test requires native invocation counters and an Arrow-only
internal topology. It uses a **test-only admission override** after checking production fallback;
it does not establish production eligibility, ordered-output parity, full-type recovery/metric parity,
or end-to-end allocation/performance results.

Synchronous batch execution now admits variable-width key/accumulator copies before encoding,
historical string-result event copies before processing rows, and Arrow gather/builder capacity
before materializing output or committing dirty state. Historical event credit includes extrema
that can become visible after retractions, not just the currently emitted value. Budget calls
remain batch-scoped rather than per-record JNI calls. These are conservative capacity estimates:
tests cover wide values, sliced/nested/decimal/dictionary gather buffers, and denial before state
commit, but this does not establish allocation-profile or performance parity for every type/mode.

Counted DISTINCT and retractable extrema also include a fixed sparse B-tree allowance: the first
value allocates an entire node, not just one key/value pair. Synchronous execution admits initial
nodes per distinct group, per-row map growth, accumulator vectors, and fixed mutation storage
before loading or decoding state. Controlled same-thread System-allocator tests measure requested
heap bytes for sparse and dense numeric/string maps, including node splits and shrink-to-empty
retractions, against the estimate. They reproduced a 464-byte first node previously charged as
112 bytes on the current test platform. Removing the last value can retain that node, so the
estimate retains base credit for empty maps too. Low-budget tests verify rejection before state
reads/writes and release of batch reservations. This is allocation
regression evidence, not an end-to-end JVM/native allocation profile or a measured throughput gain.

Synchronous aggregation uses one shared allowance for decoded historical state and its serialized
mutations. It no longer adds a second decode allowance to batch scratch for the same historical
bytes. Once the decoded maps have been consumed into mutations, it releases unused decode
headroom before allocating Arrow output, retaining credit for the serialized buffers. Incoming
state growth and output retain their separate batch allowances. This changes accounting lifetime,
not state encoding, backend access, DataFusion computation or changelog semantics. Regression
tests exercise a 500 KiB historical DISTINCT count/sum value under a 5 MiB allowance, and measure
decoded numeric/string counted maps with concurrent serialized mutations against the shared credit.

For current version-6 synchronous state, an allocation-free scan sizes historical map entries and
owned variable-length payloads instead of applying the generic eight-times-serialized-size bound.
One batch reservation covers those entries and the serialized mutation; accumulator headers and
sparse initial map nodes remain covered by the existing per-key batch allowance. The canonical
writer now allocates exactly the encoded size, avoiding geometric buffer growth while decoded maps
are live and keeping neutral states compact. The encoding and value-validation rules are unchanged.
Older state versions retain the conservative decode allowance and normal migration behavior.
Tests measure decoded Boolean, integer, floating and Unicode membership maps plus concurrent
mutations, cover every accumulator wire variant and neutral vectors, and check malformed/truncated
frames and budget denial before decoding. This is coarse state/batch accounting, with no per-row
allocator or JNI budget calls.

Canonical counted-value maps are decoded through the standard library's bulk map construction
when their entries are strictly ordered. This avoids a root-to-leaf insertion for every historical
value on every batch. Unordered legacy entries and comparator-equal duplicates retain the previous
insertion behavior, including the first key's NaN payload and the last signed count. Tests cover
these cases, version-one integer entries, truncated lengths and staging-vector memory within the
batch allowance. DataFusion still computes aggregate results; this optimization concerns the
Flink-compatible membership state adapter.

Synchronous membership-growth admission counts non-null arguments accepted by each call's FILTER
using Arrow bitmaps. Rejected or null arguments cannot insert counted state and no longer reserve
space for a prospective entry. Initial map nodes, historical state, variable payloads and output
retain their separate allowances. This is performed once per batch before state loading and keeps
nullable FILTER behavior and sliced-array offsets; no per-row state or budget calls are introduced.

Generated Calc/Aggregate/Calc tests compare `COUNT(*)`, `SUM`, `MIN`, `MAX`, and `AVG` against an
operator produced by Flink's SQL planner, including its generated handler. They cover null and
Unicode keys, null values, all four input RowKinds, empty batches, timestamp presence/values,
and logical stage counts. BIGINT boundary values exercise wrapping sums and truncating averages,
including retraction of the minimum representable integer. The same aggregate fixture participates
in full registered-metric comparisons, canonical backend switches, aligned/unaligned keyed-state
checkpoints, and 1-to-2-to-1 rescaling. Mini-batch and partial-input fixtures also include AVG.
A real Flink mailbox/network harness additionally snapshots a partly aligned two-channel input,
restores its keyed state, and replays the captured Arrow IPC frame through Flink’s channel-state
reader. It checks every resulting aggregate changelog byte, including overflow and later
retractions, on both backends with aligned and unaligned checkpoints. This evidence supports the synchronous keyed BIGINT admission subset above.

The selected synchronous aggregation node now contributes a protobuf fragment to the common
region collector instead of constructing a legacy per-operator Java runtime. Direct selected-graph
tests cover hash-keyed aggregation, singleton global aggregation, and `SELECT DISTINCT` between
Calc stages: one common keyed runtime, Arrow edges, and the original Flink stage ID/name/UID.
Fragment validation merges active table configuration with persisted node overrides, and rejects
TTL, unsupported retraction contracts, and unimplemented optional state metric configurations explicitly.
Raw mini-batch controls now use the same fragment path, with the configured bundle size preserved.

SQL integration tests additionally execute actual planner-generated source/exchange/region/sink
graphs on a Flink mini-cluster with both backends. A test-only processor first verifies that the
normal planner rejects only the outstanding architecture requirements, then applies the ordinary
selected-node conversion. Retracting DataStream inputs exercise keyed/global aggregates, DISTINCT,
and projection/filter stages around aggregation; literal VALUES exercise keyed/global aggregates.
The tests compare the complete collected external-Row changelog with Flink, require nonzero shared
native-plan batch counters and zero legacy aggregate batch counters, and reject Java transformations
for internal fragment-compatible stages. Network edges remain Arrow IPC and region outputs remain
Arrow batches. This extends integration coverage without introducing a production gate override.
Order-sensitive VALUES fixtures use one literal source: expressions inside VALUES can become
independent UNION sources, whose arrival interleaving is not deterministic across separate jobs.

The ordinary shared Flink runtime also has generated SQL-handler changelog/timestamp parity
after canonical cross-backend restore and aligned/unaligned operator-state restore on both
backends. RocksDB tests verify that a completed checkpoint's SST files are reused by the next
checkpoint. A separate generated 1→2→1 rescaling matrix routes actual Rust hash-exchange frames
across all 16 test key groups and uses Flink's checkpoint repartitioning. It checks per-key
serialized changelog order and timestamp presence/values through canonical cross-backend restore
and aligned/unaligned operator-state restore on both backends, including retractions that empty
the restored groups. These tests do not cover in-flight network replay and do not remove the
production admission gate.

For the default synchronous configuration, a metric-subtree comparison against the SQL-generated
Flink operator verifies registered names/types, deterministic record counters and watermark gauges,
and rate-meter counts/implementation on both backends, together with insertion/retraction changelog
parity. Optional metric configurations and all terminal-path semantics remain outside that test's scope.

Raw mini-batch and global partial-accumulator aggregation now bind into the shared native tree.
They use the same unary adapter, working set, controls, gauges, and state factory; composition
does not require operator-pair fusion rules. The local producer now uses the same native lifecycle
adapter and Java fragment contract. The incremental stage and bounded-final modes still need migration.
Missing mini-batch schemas, a zero-sized
streaming global bundle, and bounded-final bindings are rejected before opening a database.
Invocation EOF is not a flush or end-of-input signal. Full managed-memory and Flink metric-surface
admission, remaining control-mode migration, and release Nexmark comparisons remain unfinished. The existing
whole-plan fallback is unchanged.

The common native runtime now provides explicit stage-addressed control drains through the same
execution tree, with ordered child-before-parent processing and one Arrow output batch per pull.
Each producing kernel remains responsible for bounding and admitting its output allocation.
Raw and global partial mini-batch aggregation implement these controls with at most 2,048 groups/4,096 changelog
rows per pull. It emits the owned-timestamp v1 envelope with absent record timestamps, matching
Flink's bundle collector. Adjacent Calc and aggregate stages consume those batches directly;
synchronous aggregation also preserves owned timestamps. State boundaries reject undrained
bundles. Cancellation requires recovery and keeps returned output memory leased until release.
The versioned protobuf JNI control edge and common Flink runtime owner now schedule discovered
stage capabilities automatically: watermark drains complete before forwarding the mark,
pre-checkpoint hooks flush pending bundles, and physical-stage completion follows its children.
Full metrics and production planner admission remain unfinished.

The shared raw and global partial mini-batch kernel now prefetches missing
keys once per incoming Arrow batch and commits the latest flushed values in at most one backend
write. A decoded working set preserves each count-triggered bundle's changelog, including
delete/reinsert sequences; an unfinished tail stays pending and is not included in that write.
Regression tests exercise both native backends, exact output order across Arrow chunkings,
canonical state, absent-key retractions, and denied memory admission. Packed-state decode credit
stays live through decoding, sparse accumulator/output storage is admitted before computation,
and an emptied pending map retains its capacity charge. Global input admits and decodes the whole
opaque accumulator batch before modifying pending state. Original SQL value indices never index
the receiving key-plus-accumulator schema. These state-I/O checks do not lift planner gates or
establish a measured performance gain.

Native global-partial tests cover both backends, count triggers across Arrow chunkings, one
read/write batch per incoming batch, malformed/denied decode before state access, 5,000-key bounded
control drains, per-stage logical counters and bundle gauges, retained output leases, and canonical
cross-backend restore followed by signed partial retractions. These are shared-runtime checks,
not full-type/metric parity, in-flight unaligned channel replay, or a complete allocation profile.

A generated JNI test drives the global consumer through `Calc → GlobalGroupAggregate → Calc`
with one-record local bundles as opaque input fixtures. It compares the ordered serialized
changelog against Flink's SQL-generated raw aggregate handler at equivalent receiving bundle
boundaries: both backends, three triggers, three seeds, nullable Unicode keys/BIGINT payloads,
all four raw RowKinds encoded into signed partials, empty arrivals, watermarks, pre-checkpoint
flushes, and finish. Logical stage counters, absent timestamps, and released memory are checked.
The local fixture producer uses its retained API outside the shared input edge; this test does
not establish fused local production, two-phase SQL planner selection, or full metric parity.

The selected Java global node now implements the shared fragment/metadata contract and declares
keyed state ownership. Its former per-operator runtime translator has been removed. The ordinary
region collector absorbs the exchange reader and composes adjacent Calc stages without translating
an intermediate Java operator. Topology tests preserve the original global physical ID, metric
name/UID, configured bundle size, and raw SQL accumulator indices even when those indices are
outside the receiving opaque-input schema. Table configuration and persisted overrides are merged
before validation; unsupported TTL, async/changelog state, retraction contracts, malformed partial
schemas, and optional state-metric requests reject the fragment instead of silently changing behavior.
This global-fragment change alone does not establish a complete two-phase SQL plan.

The ordinary Flink region owner is also tested with global fragments from the production builder:
automatic watermark/pre-barrier/end/finish drains, canonical cross-backend restore and native aligned
and unaligned state restore, followed by signed-partial retractions. A separate matrix compares the
complete registered **default** bundle metric surface at equivalent Flink bundle boundaries, including
empty arrivals, pending bundles and flushes, on both backends and three count triggers. The input
fixtures use one-record local bundles and Flink's SQL-generated raw handler as an equivalent receiving
bundle oracle, not a complete local/global SQL pipeline. These checks do not cover in-flight channel
replay, optional RocksDB/state-latency metrics, full-type restoration, or end-to-end allocation profiles.
Global composition is admitted independently of the still-active persistent-memory gate.

The retained local producer now admits protobuf decode, planned Arrow schemas/codecs, batch derivatives,
accumulator updates and opaque output construction before allocation. Workspace credit moves into
retained state without re-admission, and flushing keeps the actual hash-table allocation charged.
The same tombstone-safe table accounting is used by raw/global bundles and native memory keyed state.
Invalid RowKinds and denied workspace do not change the prior local bundle; computation/transfer
failure frees mutated storage before its credit and requires recovery. Controlled Rust heap tests
check constructor and batch live/peak coverage, denial and cleanup.

Local aggregation now also registers its replayable task-local buffer automatically in the shared
native context, before control/metric capability negotiation. It uses the existing unary
DataFusion adapter and stream driver, with no operator-pair recognition or separate keyed backend.
The factory copies only its own configuration, not a serialized child sub-plan. Count-triggered
partials and explicit control output carry INSERT kinds and absent owned timestamps; invocation
EOF does not flush. Control output is pulled in at most 2,048 groups per batch. Canonical keyed
snapshot requests to the local node reject explicitly; Flink must drain its bundle before a
checkpoint and rebuild it through replay.

Native tests compose `Calc → LocalGroupAggregate → Calc → GlobalGroupAggregate → Calc` with
memory and RocksDB global state, verifying child-before-parent controls, stage logical counters,
bundle gauges, cross-backend restore/retractions, and no Java ownership transfer internally.
Additional tests compare count-trigger/retraction partial bytes with the retained local kernel,
check 5,000-key bounded control drains and fail-closed cancellation, and retain output leases after
tree destruction. A same-thread heap check covers shared-context construction, count-triggered output
and control drains against admitted peak/live credit; invalid late RowKinds preserve the prior bundle
and poison the invocation for recovery. This is native composition evidence, not a Flink SQL local/global parity oracle,
full-type/metric or in-flight checkpoint coverage. A complete allocation profile remains unfinished;
production fallback and Nexmark performance claims are unchanged.

The selected streaming local node now contributes a protocol-v2 fragment to the ordinary region
collector; its standalone streaming runtime translator has been removed. It preserves original
physical metric identities during local/global and incremental rewrites, merges table settings with
persisted overrides, and rejects malformed grouping/opaque schemas, disabled or invalid mini-batching,
and unsupported call/retraction contracts. It declares no keyed-state ownership.

The common one-input Arrow owner now uses the same discovered control scheduler and gauge publication
as the multi-input owner. Its planned input schema allows control delivery before the first row;
watermark, pre-barrier and end/finish callbacks drain through the shared tree. Envelope requirements
are negotiated once from native task-lifetime resources, independently of keyed state bindings, so
local changelog RowKinds are not lost. Actual routing/envelope field names remain reserved, but the
planner's `__streamfusion_accumulator` is an opaque payload rather than routing metadata.

Generated SQL tests now exercise complete Flink local/exchange/global graphs through a **test-only**
selection probe: memory/RocksDB, keyed/singleton aggregates, three bundle sizes and three seeds, null
and Unicode keys, nullable BIGINT payloads, all four RowKinds, and retractions that empty the groups.
They compare the complete external changelog bytes, require nonzero shared-plan and zero legacy
local/aggregate invocation counters, and check Arrow topology plus original stage identities.
A separate common-unary harness compares the complete default local metric surface with Flink's
SQL-generated `MapBundleOperator`, including count triggers, empty arrivals, watermark/pre-barrier
drains, finish and timestamp-less INSERT partials. This is not full-type, arbitrary failure-path,
two-phase in-flight recovery or end-to-end allocation/performance parity. Production admission is limited to the synchronous keyed BIGINT and VARCHAR extrema subsets described above.

The generated retained-kernel JNI test also compares ordered `RowDataSerializer` changelog bytes
against the original SQL-planned Flink mini-batch operator for both backends, three seeds and
three count triggers. It covers nullable Unicode keys and BIGINT values, all four RowKinds,
empty inputs, count and final-watermark flushes, at-most-one read/write per Arrow input, and
released native/Arrow memory. It is not full-type, full-metric, or fused-control parity evidence.

A separate shared-tree JNI test now compares ordered serialized changelogs and absent record
timestamps against that SQL-planned Flink operator through `Calc → GroupAggregate → Calc`, on
both backends with three count triggers. It drives explicit watermarks, pre-checkpoint flushes,
and finish, verifies per-stage logical I/O counts, and checks empty arrivals and memory release.
Rust tests also cover 5,000-key bounded drains through adjacent aggregate stages and canonical
cross-backend restore followed by retractions. These tests exercise the common native control
API directly; they do not prove the complete mini-batch gauge surface.

The common Flink runtime is also tested through its actual watermark, pre-barrier, end-input,
and finish hooks against SQL-generated Flink changelogs. This includes canonical restore between
memory and RocksDB, aligned/unaligned state checkpoints, incremental RocksDB checkpoint reuse,
output-before-watermark ordering, idempotent completion, and memory release. Branch scheduling
tests cover stalled inputs and all-idle transitions that advance an ancestor watermark twice;
the scheduler preserves separate ordered control waves. In-flight network replay remains outside
this coverage.

Mini-batch bindings now expose Flink's `bundleSize` (Integer) and `bundleRatio` (Double) through
the common native gauge schema and bulk snapshot, with stable physical-stage identities. Metric
reporters read Java snapshots without per-gauge JNI calls. Generated tests compare the complete
default registered operator metric subtree and ordered changelogs on both backends, at batch and
control boundaries, over three count triggers and six seeds. They cover pending/empty inputs,
count/watermark/checkpoint flushes and completion. Recovery tests check bundle gauges after
canonical cross-backend restore and aligned/unaligned state checkpoints. Native tests also verify
independent gauges for adjacent aggregates and released snapshot allocations. Optional metric
configurations remain explicit planner restrictions; this does not open production admission or
prove per-row sampling inside a vectorized batch.

The shared fragment builder now preserves configured raw mini-batch sizes. The generic region
composition check admits streaming aggregation alongside other verified families; it adds no
operator-pair rules. An injected downstream failure after 2,048 emitted flush records matches
Flink's partial serialized output and complete default metric surface on both backends, without
forwarding a watermark. Subsequent native control/checkpoint work requires recovery, and failure
close releases native memory without flushing the remaining bundle. Arbitrary row-interior
failure equivalence is not established. Enabled keyed-state latency histograms and RocksDB native
property/statistics metrics still reject the shared fragment with a precise metric-specific reason.
Large-owner admission, backend configuration, recovery, and metric conformance still gate production selection.

The real-SQL graph probe now also runs one-phase raw mini-batch keyed/global queries through
source, Arrow exchange, common native region and sink on both backends, comparing changelog bytes
against Flink and requiring non-zero shared native invocations with zero retained per-family
aggregate invocations. This probe still converts graphs only after asserting that production
fallback reasons are architectural; it is not proof that the production planner gate is open.

## SQL example

```sql
SELECT bidder, COUNT(*) AS bids, SUM(price) AS spend,
       MIN(price) AS minimum_price, MAX(price) AS maximum_price
FROM bid
GROUP BY bidder;
```

## Acceleration and fallback

StreamFusion accelerates keyed `StreamExecGroupAggregate` plans containing `COUNT(*)`,
single-input `COUNT`, and the following aggregate/type combinations. Every listed call also
supports SQL `FILTER (WHERE ...)`; null filter predicates are false.

| Aggregate | Supported Flink SQL types |
| --- | --- |
| `SUM` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL` |
| `AVG` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL` |
| `MIN`, `MAX` | All `SUM` types plus `BOOLEAN`, `CHAR`, `VARCHAR`, `DATE`, `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ` |

`COUNT(DISTINCT value)` supports the same scalar types as `MIN`/`MAX`, and
`SUM(DISTINCT value)` and `AVG(DISTINCT value)` support the numeric types above. Distinct values
are counted in native state, so duplicate
inserts and retractions change the result only at first/last membership boundaries. `DISTINCT`
and `FILTER` may be combined.

Input may be insert-only or a Flink changelog. Changelog output preserves Flink's per-record
`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` behavior byte-for-byte; unchanged aggregate
values do not produce a spurious update. Retraction parity covers null values, duplicate extrema,
removing the current extremum, deletes against absent state, IEEE-754 NaN and signed zero, and
deleting the final row of a group. Integer and decimal arithmetic uses the planned Flink result
type, including its overflow behavior.

Global aggregates use Flink's required singleton exchange and the canonical zero-field
`BinaryRowData` state key. The native batch path materializes that key once rather than hashing one
identical allocation per input row. Flink's `StreamExecExpand` is accelerated for grouping sets,
`ROLLUP`, and `CUBE`; the expanded grouping fields may widen from non-null to nullable without
changing their logical type. Generated parity cases cover scalar binary, decimal, date, and
timestamp keys as well as opaque array and row keys through that nullable boundary. Native Expand
derives each output field's nullability across every projection, including a later grouping-set
projection that replaces a non-null input reference with a typed null.

One- and two-phase count-triggered mini-batch aggregation are accelerated. Flink's split-DISTINCT
three-stage shape is also accelerated for the aggregate/type combinations above. It remains three
observable physical stages—native local aggregate, native stateful incremental aggregate, and
native global aggregate—with the two planned exchanges retained between them. The incremental
stage keeps counted DISTINCT membership and retractable extrema by partial key, merges local opaque
deltas, and emits one opaque net accumulator delta per final key. Ordinary COUNT/SUM/AVG and
append-only extrema remain bundle-local, so they do not create keyed-state reads or tombstones.
Duplicate values and extrema replacements therefore remain correct across bundle boundaries,
including after retractions and recovery.

The two-phase shape is
lowered as distinct native local aggregate, native exchange, and native global aggregate plan
nodes. The local stage is state-free and emits grouping columns plus one opaque, versioned native
accumulator; neither the exchange nor Java interprets that accumulator. The global stage merges
the partial deltas into the same canonical keyed state used by one-phase aggregation. StreamFusion
preserves Flink's exact bundle boundaries even when one Arrow input batch crosses several
boundaries, buffers opaque group keys and accumulator deltas in managed native memory, and emits at
most one aggregate-level change per key when a bundle is finished. Bundles finish at their
configured count, before a watermark or checkpoint, and at bounded input completion.
Processing-time and row-time mini-batch assigners remain Arrow control operators and never
transpose the payload back to rows. Local bundle output follows Flink's Java `HashMap` bucket
iteration order because that order can affect the receiving global bundle boundary and therefore
the observable changelog. Native `Expand` emits projection results in Flink's input-row-major
order; projection-major union batches are not used because they would alter local and incremental
bundle boundaries.

Bounded `BatchExecHashAggregate` and the local/global hash- or sort-aggregate shapes use the same
native accumulator kernels. A bounded local stage emits one opaque partial accumulator per touched
key for each incoming Arrow batch. The framed Flink exchange transports that internal Arrow schema
without Java interpretation; the receiving global stage decodes the frame inside the aggregate
task, performs one distinct-key multi-get and one mutation batch, and emits final `INSERT` rows only
after end of input. This also covers bounded grouping sets, `ROLLUP`, and `CUBE`: native Expand is
retained below the local stage, while Flink's physical pre-aggregate sorts and exchanges are folded
into the equivalent native local/exchange/global tree. Bounded SQL input is append-only by Flink's
physical contract. Changelog retractions remain supported by the streaming aggregate path.

State TTL, async state, Flink's changelog-state wrapper, multi-column `DISTINCT`,
ordered or approximate aggregates, `IGNORE NULLS`, unsupported aggregate functions, and
unsupported aggregate value types fall back with a specific EXPLAIN reason. Flink's internal
`SUM0` call uses the same accumulator as `SUM` here: a group has no output after its last row is
retracted, so an empty accumulator value is not observable in this physical operator.
Flink may instead retain `AVG` as a physical aggregate call. That path uses Flink's two-buffer
contract: a wrapping `BIGINT`, `DOUBLE`, or `DECIMAL(38, input_scale)` sum and a `BIGINT` non-null
count. Decimal results apply Flink's precision-38 `HALF_UP` division and final-scale rounding.
Both ordinary and counted-distinct AVG state support filters and retractions.

Grouping keys use the same Flink `BinaryRowData` bytes and key-group assignment as native
deduplication. Scalar keys are encoded directly in Rust; keys that need Flink's complex internal
encoding are supplied as opaque bytes. Java never interprets native state keys or accumulator
values.

## State and recovery

The aggregate uses the shared backend-neutral native keyed-state interface:

- `HashMapStateBackend` stores opaque values in `ahash`/`hashbrown` maps split by key group.
- Flink's bounded in-memory keyed backend, identified by Flink as `batch`, uses that same opaque
  native memory implementation and canonical format. When the existing `state.backend` setting is
  `rocksdb`, a bounded native aggregate honors it even though Flink presents its wrapper backend as
  `batch`; no StreamFusion-only backend selector is required.
- `EmbeddedRocksDBStateBackend` talks to the separately packaged RocksDB component through its
  versioned native ABI. The immediate path performs one distinct-key multi-get and one `WriteBatch`
  per Arrow input batch, including deletes. Raw and global partial mini-batch aggregation perform at most one
  missing-key read and one mutation write per incoming Arrow batch while preserving exact Flink
  bundle boundaries in their output. Other retained partial-accumulator stages require separate audits.

Both backends use the same versioned canonical key-group snapshot format. Accumulator payload
version 6 adds sparse neutral-accumulator tags while continuing to read versions 1–5; version 5
added ordinary and counted-distinct AVG sum/count state, version 4 added counted `DISTINCT` sets,
and version 3 introduced typed boolean,
floating-point, string, temporal, and nullable decimal-overflow state.
Canonical savepoints are tested across all four source/target
backend pairs and redistribute key groups during both 1-to-N and N-to-1 rescaling. Regular RocksDB
checkpoints use native file snapshots and Flink keyed-state handles. Incremental checkpoints reuse
completed immutable SST files; full checkpoints upload every file privately, following Flink's
existing incremental-checkpoint setting. Both restore into native RocksDB. Generated DISTINCT tests
cover full aligned/unaligned restore and rescaling; canonical savepoints remain backend-neutral.
Shared group aggregation imports physical files as admitted Arrow key/value pages and batched
state writes, preserving large individual legacy values when the host budget permits them.
Generated parity tests also cover full/incremental restore of an 8,192-member hot group followed by
complete retraction and recreation. Canonical savepoints still require whole-key-group buffers;
see [native state](/StreamFusion/development/native-state/) for the limits and ABI-9 upgrade requirement.

Global aggregate recovery is independently tested for all four memory/RocksDB source-to-target
backend pairs with canonical savepoints and with both aligned and unaligned checkpoints. Global
state remains singleton state; keyed and grouping-set aggregates retain the normal Flink key-group
rescaling contract. The two-phase global operator uses that identical snapshot/checkpoint path;
canonical native global state, including bounded final-output state, is also round-tripped between
the memory and RocksDB processors. The
local stage flushes before the checkpoint pre-barrier and has no persistent state of its own. The
stateful incremental stage uses the same canonical raw-keyed snapshot and direct RocksDB ABI as
the global stage. Native tests cover duplicate/retraction restoration after key-group rescaling,
memory-to-RocksDB restoration, batched state reads and writes, state-free ordinary split branches,
and managed memory admission. Its pending bundle is flushed before checkpoint snapshotting by the
shared group-aggregate operator lifecycle.

The aggregate operator has no timers in the immediate, two-phase, or split-DISTINCT
count-triggered mini-batch shapes.
A pending mini-batch is finished before the checkpoint pre-barrier hook, so aligned and unaligned
checkpoints snapshot the same canonical aggregate state; Flink's channel-state machinery owns
messages still in flight and the native state does not need a second message sequence log. The
processing-time mini-batch assigner uses Flink processing timers only to advance its batch
watermark. With aligned checkpoints, exchange rows are gathered into one Arrow frame per non-empty
destination. With unaligned checkpoints, exchange frames retain a single key group so Flink can
redistribute captured channel state during rescaling without splitting a frame.

Arrow buffers, in-memory state, aggregate scratch/output storage, RocksDB cache and write buffers,
and restore readers are admitted through the operator's existing Flink managed-memory allowance.
Used, peak, and limit gauges are exposed under the operator's StreamFusion metric group. The same
group reports logical processing/changelog counts and the native processor's actual batched state
reads and writes,
checkpoint kind/bytes/duration/failures, incremental upload and SST-reuse bytes, and restore
bytes/duration/failures.

In the shared raw mini-batch path, bounded control-drain scratch is based on visible result payloads
and serialized mutations rather than multiplying already-charged retained B-tree storage. Canonical
sizing is allocation-free and shares the encoder with persistence, with no state-format change.
The sparse-extremum unit case (2,048 groups) reserves less than a quarter of the previous scratch
allowance. This is an admission-size comparison, not measured throughput. Controlled native heap
observations cover shared-tree lifetimes with numeric groups and nullable/wide-string hot keys;
production admission for mini-batch aggregation remains unfinished.

Stateful aggregate stages declare a larger Flink `OPERATOR` managed-memory weight than stateless
Arrow stages, while the bounded local bundle declares a smaller intermediate weight. These are
relative Flink operator weights, not a StreamFusion memory pool or deployment setting. They keep
persistent grouping and DISTINCT state from inheriting the same per-slot share as a transient Calc
buffer; allocation remains fail-fast when the resulting Flink allowance is exhausted.

Retractable `MIN` and `MAX` keep counted ordered values so deleting the current extremum reveals the
next one. Insert-only extrema use a single scalar instead. The immediate batch path deduplicates
keys with `ahash`, decodes each touched accumulator once, and applies every row in input order. The
raw mini-batch path performs one backend multi-get for missing keys and at most one mutation batch
per incoming Arrow batch, while preserving Flink `HashMap` iteration order inside each completed
bundle. Global partial-input mini-batches use this same batch working set. This follows the keyed
aggregate-group/cache shape used by RisingWave and Arroyo's Arrow incremental aggregates while
retaining Flink's immediate or mini-batch changelog contract as planned.

The RowData Nexmark `group-aggregate`, `global-aggregate`, `grouping-sets`, and
`incremental-group-aggregate` harnesses compare Flink and StreamFusion in separate JVMs for both
state backends. The first three also have bounded-runtime parity coverage through the same
Kafka-free RowData source/sink boundary. Benchmark builds use the Rust release profile and the
build machine's native CPU feature set. The harness records elapsed time, input throughput, native
calculation batches, native local-aggregate batches, and native stateful aggregate batches so a
two-phase run proves that both stages executed and exchange fragmentation/JNI call amplification
remain visible.

The September 4, 2026 split-DISTINCT measurement covers implementation commits `00d5bfe` and
`c946c63`. Three alternating fresh-JVM forks processed one million deterministic events at
parallelism one with a 3GB heap, 1,024 MiB of Flink managed memory, count-triggered mini-batching,
and one-second exactly-once checkpoints. In-memory medians were 86,319 events/s for Flink and
81,639 events/s for StreamFusion (94.6% parity), with elapsed ranges of 11.585–12.963s and
12.152–12.537s. RocksDB medians were 70,449 and 77,324 events/s respectively, a 9.8%
StreamFusion gain. RocksDB ranges were 13.760–19.417s and 12.745–28.031s; those wide ranges are
retained because one storage/checkpoint outlier occurred on each side in repeated runs. Every
StreamFusion fork reported acceleration and nonzero native Calc and aggregate batches. Every fork
materialized 19,913 rows with SHA-256
`43fc431b4c20fd8c9cf6aacae7f0c4b7332e6f470b4386cf910d66301081ad49`.

A two-million-event mixed JVM/native CPU profile attributed 28.4% inclusively to the required
RowData-to-Arrow source boundary, 34.7% to the incremental aggregate, and 21.8% to the local
aggregate. Within the incremental stage, finishing a bundle accounted for 19.6% and accumulator
application for 9.3%. Allocation samples led to retaining grouping rows only for newly observed
keys and moving final-map keys and rows instead of cloning them. Java JFR showed filesystem sync
waits from asynchronous checkpoint completion as the largest sampled native method; native RocksDB
checkpoint creation no longer performs a redundant explicit flush before RocksDB creates its
checkpoint. Profiler-instrumented timing was excluded from the throughput measurements.

On the September 4, 2026 local one-million-event two-phase run at parallelism four and bundle size
5,000, three alternating fresh-JVM forks produced in-memory medians of 184,336 events/s for Flink
and 182,799 events/s for StreamFusion (99.2% parity). RocksDB medians were 170,047 and 171,154
events/s respectively, a 0.7% StreamFusion gain. Elapsed ranges were 5.394–5.501s and
5.389–6.416s in memory, and 5.844–8.641s and 5.825–5.874s on RocksDB; the isolated slow fork on
each side is retained as observed variance. Every StreamFusion fork reported acceleration and
nonzero native Calc and aggregate batches. Exact changelog parity is established by deterministic
SQL and recovery tests because the one-second benchmark checkpoint can finish a bundle at a
different input position in separate wall-clock runs, changing valid intermediate updates without
changing final keyed results.

Mixed JVM/native CPU profiles on two-million-event forks retained JFR, collapsed stacks, per-engine
flame graphs, and differential flame graphs. The StreamFusion memory profile attributed 3.2% of
samples to the local aggregate, 3.1% to the global aggregate, 1.1% to Arrow C import/export, 7.4%
to the required source RowData-to-Arrow boundary, and 4.1% to the keyed exchange. Direct native
RocksDB occupied 0.6% of StreamFusion samples versus 2.0% for Flink's RocksDB path, so additional
bundle-boundary state-call coalescing was not justified by this profile. Sampled Java allocation
volume was 5.23/5.26 GB for StreamFusion memory/RocksDB versus 14.48/14.61 GB for Flink. Separate
native-allocation profiles attributed 63 MB in memory and 77 MB on RocksDB to the two native
aggregate stages over two million events; JVM compiler arenas, Arrow boundary buffers, and exchange
buffers were larger. Profiler timings were excluded from throughput results.

On the September 3, 2026 local one-million-event run, three alternating fresh-JVM forks gave global
aggregation in-memory medians of 107,827 events/s for Flink and 103,550 events/s for StreamFusion
(96.0% parity). RocksDB medians were 99,061 and 106,810 events/s respectively, a 7.8% StreamFusion
gain. Elapsed ranges were 9.177–9.689s and 9.458–9.854s in memory, and 9.844–10.893s and
9.324–10.166s on RocksDB. Every fork emitted 1,839,999 changelog rows with SHA-256
`def58ec236efbd1b8d4230f25681e86ef79a487155cd47791631558c0d9d299a`.

Grouping sets on the same event count produced in-memory medians of 75,963 events/s for Flink and
78,222 events/s for StreamFusion, a 3.0% gain. RocksDB medians were 57,959 and 71,869 events/s, a
24.0% gain; the StreamFusion RocksDB elapsed range of 13.473–17.165s is retained because storage
variance was visible. All forks emitted 3,650,083 rows with SHA-256
`25375393fce85edf36dd09d91a045ff75af3a0470896ff1d242ec447058a86ea`.

Mixed JVM/native CPU profiles used non-safepoint Java sampling, DWARF/frame-pointer unwinding, JFR,
collapsed stacks, flame graphs, and differential flame graphs. The complete native global
aggregate path was 1.2–1.3% of CPU samples after removing per-row empty-key hashing; grouping sets
placed 2.2–2.3% in aggregation, about 6% in its native Expand stage, and 7–8% in Arrow/RowData
boundaries. Direct RocksDB was 2.7% of the grouping-sets profile. At 500,000 events, sampled Java
allocation volume was 3.02/2.97 GB for StreamFusion global aggregation versus 4.82/4.98 GB for Flink
on memory/RocksDB, and 4.04/4.17 GB versus 6.73/7.24 GB for grouping sets. Native allocations were
led by required Arrow output and transport buffers; direct RocksDB allocations were not dominant.
Profiler timings were excluded from throughput results.

See the [Flink 2.3 Group aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/).
