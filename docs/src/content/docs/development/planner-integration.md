---
title: Planner integration
description: The minimal hook used to select StreamFusion planning logic.
---

Replacement is committed only after all roots have been constructed successfully. Retained
Flink source and sink edges are staged during conversion; a conversion failure leaves the
original graph intact. Shared nodes retain one replacement identity across roots. Sharing a native
stage internally across regions is separately rejected until multi-output native ownership is
implemented: independent unary collection would otherwise execute that stage more than once.
EXPLAIN names its consumers, and every root remains on Flink. Source-boundary sharing and multiple
consumers of a complete region output remain supported.

## Native regions

Selected Calc, Expand, collection-UNNEST, and streaming row-replication nodes now expose a shared
native-plan fragment interface. The unary region collector follows that interface rather than
enumerating operator pairs or subclasses, and rejects cycles. Each Java-side
stage produces its own versioned protobuf fragment; the region composer connects their native inputs,
then creates one Arrow-input/Arrow-output runtime operator at the region edge. Internal stages
are never translated to separate Java runtime operators. Streaming and bounded stages are not
mixed into one region.

The former Calc/UNNEST and Calc/row-replication combination builders have been removed from
the runtime translator as well as planner dispatch. Their structural tests now use the same
fragment composer as other unary stages; adding a stage does not require adding combinations
to Calc. Planner-to-runtime topology tests exercise both streaming and bounded selected graphs,
asserting one native Arrow operator, one source-edge translation, and every internal protobuf stage.

Generated tests compare Calc → Expand → Calc and Calc → UNNEST → Expand → Calc against Flink's actual
code generators and collection kernel for complete serialized changelogs, including all four
RowKinds, and per-stage logical-record counts across batch sizes. Native handoff tests
assert shared Arrow array and buffer identity across Calc stages around Expand, which allocates
its expanded output. This implementation foundation does **not** lift the current mixed-region
production admission restriction: large-buffer and retained-state admission and metric-surface validation
remain required, as does general composition with stateful operators. See
[architecture admission](/StreamFusion/development/architecture-admission/).

Streaming regular joins and deduplication now implement that same fragment contract and declare
keyed state ownership. The collector binds all state-node identities to one generic Flink keyed
multi-input runtime, including unary stateful trees. The former lifecycle-owner fusion interface
has been removed; no operator builds a special runtime for its neighbors. Existing exchange readers
are absorbed into the region edge: the runtime receives their original IPC frames and decoding
contracts without a decode/re-encode pair. Keyed regions require planned external exchanges with
matching distribution, maximum parallelism and routing parallelism; missing or incompatible routing
is rejected, never inferred from an operator name. Flink's synthetic frame key selects the already
routed key group; Rust still computes actual state keys and key groups.

The global partial-accumulator aggregation consumer now uses that same fragment contract and
state/control owner. Its former standalone Java runtime translator is removed; no collector,
neighbor-specific fusion rule, or control-dispatch branch was added for it. Effective table settings
and persisted overrides, original physical metric identities, retraction requirements and state TTL
are carried into fragment validation. The streaming local producer now uses this fragment contract
as well, with no keyed-state owner. Incremental producers and complete production memory admission
remain unfinished; two-phase SQL currently runs through this path only in explicit selection tests.

Native task-lifetime resources need not imply a keyed backend. The shared context now discovers
local aggregate buffers before capability negotiation, and merges later Flink-supplied keyed
bindings without replacing them. Both use the same recursive lowering, unary stream adapter,
child-before-parent control scheduling, metric traversal and cancellation guard. The constructor
registry describes single operators, never neighbor combinations. Local buffer construction clones
only the node's configuration through the descriptor-generated copier also used by keyed binding,
not its child subtree; keyed snapshot methods remain unsupported for
that buffer. Native local/Calc/global composition is tested on both global state backends.
The native unary stream adapter also supports kernels that pause an incoming batch while draining
bounded output chunks. A kernel retains its admitted input cursor; the adapter drains it before
polling the next child batch or applying a watermark/checkpoint control. Shared Arrow buffers stay
owned through those pulls. Cursor failure, cancellation, non-progress and incompatible output
schemas require recovery. Existing single-output kernels keep their direct path without an extra
pending-output poll. This enables local-window pressure flushing without a separate execution driver;
window lifecycle migration and admission remain unfinished.

The common one-input Arrow runtime now uses the same control scheduler and native gauge schema as
the multi-input runtime; no operator-family control branch was added. It receives the planned input
type before open so control events can run before the first data batch. Local fragments require
protocol v2 even without an adjacent Calc. Native record-envelope requirements are negotiated once
at context creation, not inferred from the presence of a keyed backend. Two-phase conversion registers
the local physical stage with the ordinary rewrite identity/UID registry. The verified HOP subset
is admitted through ordinary planner selection.

Direct selected-graph tests check join → Expand → Calc → deduplication as one runtime with two
state identities and only external IPC writers. Runtime tests consume hash-exchange frames and
restore two deduplication owners across memory/RocksDB canonical savepoints. These do not establish
production admission, arbitrary key-changing internal transformations, timer recovery, or unaligned
channel replay. Bounded joins retain compatibility Calc-tail adapters, and other state/control
families still need migration and complete memory/metric validation.

New Calc stage fragments use plan protocol version 3 and declare that projections describe SQL
payload only. Calc and Expand declare the shared `clear_record_timestamps` stage policy, matching
Flink's generated SQL collectors: record timestamps are discarded, but SQL rowtime payload fields
and RowKinds are preserved. Common native lowering applies the policy with an ordinary DataFusion
projection inside the stage's metric identity. It forwards payload arrays directly and admits only
the newly materialized metadata through the shared projection memory path; no neighbor-specific
fusion rule or intermediate Java handoff is involved. Explicit RowKind metadata is required rather
than inventing INSERT semantics. The resulting envelope uses the existing owned v1 representation.
Protocol-3 input edges retain timestamp and RowKind arrays in that same representation, reordering
only shared array references. This gives UNION branches a compatible schema even when one branch
drops record timestamps and another preserves them. No timestamp is inferred from SQL payload.
Calc, Expand, collection UNNEST, and row replication use the common envelope schema contract, so
stateful output metadata is neither projected away nor mistaken for a correlated payload field.
Composition retains the highest parent or input protocol version and requires at least version 2 when attaching
fragments above an existing subtree, including an expansion-only tail without Calc.
Version-1 explicit-metadata and version-2 envelope-preserving plans remain readable. A timestamp
policy in an older protocol is rejected. Runtime preflight requires plan protocol 3 support, so an
older native library cannot silently ignore the record policy.
The exchange wire protocol and canonical state formats are unchanged.

Generated `Join → Expand → Calc → Calc` tests compare full serialized changelog multisets per
incoming batch with Flink on both native memory and RocksDB, covering inner/full joins and all four
input RowKinds. They compare each physical stage's logical I/O counts and require memory to return
to its initial allowance at close. This is not complete large-owner lifetime or metric-surface coverage.


StreamFusion uses a small Flink patch for planner-factory selection, exec-graph replacement, and
complete-pipeline resource finalization. The pipeline callback runs after `StreamGraphGenerator`
has seen the complete pipeline, before JobGraph serialization. This additional Flink boundary is
needed because operators added after SQL translation can change the original local-window memory
share and thus its observable pressure-flush output. It does not execute an intermediate operator
or add a JVM/native data-plane crossing.

The callback uses the existing planner-factory selection and lives in the runtime classloader;
it does not reach into Flink's isolated planner loader. It installs resolved native factory copies
only after all local resource owners succeed, leaves reusable transformations intact, and rejects
serialization of unresolved local-window resources. Tests generate repeated pipelines with different
external weights and verify that earlier job graphs keep their original capacities. The distribution
runner installs the matching patched `StreamGraphGenerator` class alongside the planner/API patches.

EXPLAIN reads planning diagnostics through the actual Flink planner's classloader. In a
distribution, these diagnostics live inside the isolated planner loader and may be invisible to
the application's context classloader. Both acceleration and precise fallback reports remain
available at that boundary. The launcher integration job includes EXPLAIN in its failure output
when a submitted Calc does not execute natively.

Aggregate, regular-join, window, and OVER plan builders live in `streamfusion-flink-planner`
under its `planner.aggregate`, `planner.join`, `planner.window`, and `planner.over` packages.
Their Calcite and Flink planner dependencies stay inside the isolated planner loader. Runtime operators receive protobuf plans
and runtime types. A source guard rejects new planner dependencies in those runtime families.
The Flink loader patch delegates the `tech.streamfusion` namespace component-first: planner
classes come from the component, while shared runtime, Arrow wrappers, and protobuf classes
come from the runtime owner. This does not expose Calcite through the runtime loader.

Both modules publish normal Maven JARs for compilation and explicit `-bundle.jar` distribution
artifacts. The runtime bundle owns Arrow, native bridges, and protobuf dependencies; the planner
bundle contains only planner classes and relocates protobuf references to match the runtime.
The launcher installs those matching bundles plus the independently packaged RocksDB native JAR,
and allocates two task slots for parallel fixtures. It checks native Calc, shared-plan UNION inputs,
whole-plan fallback, and shared HOP COUNT/attached MAX followed by a binary join on both state
backends. The shared-window fixture compares complete collected rows with a Flink baseline and
requires common-runtime native activity with no standalone local-window calls.
This packaging check supplements the generated SQL parity tests;
it does not establish coverage for operators still gated by their semantic or runtime contracts.

The `streamfusion-flink` module supplies the planner-side integration under `tech.streamfusion.flink`. Tests can select the StreamFusion implementation for one execution and clear that selection for the native Flink baseline.

Keeping the hook small matters: the target is to follow Flink's architecture and release line closely, not maintain a broad planner fork. Changes to the upstream patch should therefore be isolated, tested by the SQL harness, and reviewed independently from native operator work.

The selected streaming global-window node now implements the shared native fragment contract.
Its builder validates append-only UTC HOP partials, supported DataFusion grouped calls, canonical
bounds, and effective state/metric configuration before producing the protobuf. Shared region
composition retains the original physical node's identity and discovers its keyed-state owner;
no standalone global-window Java execution operator is created. Selected topology tests verify
Calc → global window → Calc with one keyed Arrow runtime, and generated Flink parity/recovery
fixtures use the same builder. Selected local-window nodes now join this contract and attach the
original resource calculator to the complete-pipeline finalizer. Their derived local/exchange nodes
retain original identity, and source-side local regions consume Arrow directly through the shared
runtime's single input. Generated direct and attached HOP SQL tests require ordinary planner selection on both
backends. The verified subset now admits Q5, whose release performance checkpoint is pending; see [Window aggregation](/StreamFusion/operators/window-aggregation/).

Native reuse inspection now builds a graph-wide region layout before replacement. Each physical
stage appears once, with ordered references to internal stages or external input ports. Separate
external edges retain separate Flink input ports even when their source identity is the same;
channel scheduling and checkpoint state must not be merged by native reuse.
Region exits include intermediate results used outside the region as well as terminal stages.
Generated SQL topology tests cover a reused HOP COUNT feeding both an exchange and an attached
local MAX, with one shared owner and two exits. Synthetic DAG tests cover reconverging branches,
repeated roots, and repeated input references. Fusion that would make a region consume its own
output through a Flink exchange/control boundary is rejected with an EXPLAIN reason.

The version-1 `NativeRegionPlan` protobuf now represents the selected layout as a flat list of
identified operators and explicit input references. Each operator fragment contains only anonymous
local input slots; repeated stage references share one physical definition. Ordered output IDs
can name an intermediate stage. Every external reference identifies a distinct Flink channel.
The existing protocol-3 owned Arrow envelope is required at region edges; protobuf carries only
the control plan. Java composition and Rust decoding reject duplicate identities, missing or
forward references, unused channels, unreachable stages, and malformed fragments. Rust reserves
the decoded graph under one coarse Flink memory-pool reservation before decoding. A shared wire
fixture and generated SQL layouts check both implementations.

Native region lowering now builds one retained DataFusion graph from this contract. Ordinary
edges remain direct native children; only reused stages receive bounded shared readers. The
first reader executes the producer once per invocation, and a cooperative output driver yields
a port ID alongside each Arrow batch. Different exits retain their own schemas and shared
buffer ownership. Metric snapshots enumerate the physical definitions once, including shared
stages. The driver fails incomplete consumption, cancellation, producer errors, and panics;
a completed output handle cannot release a later invocation. Reconverging shared branches
remain rejected because a downstream operator could drain its inputs sequentially and deadlock
a bounded broadcast. Supporting that subset requires demonstrated cooperative input draining.

Native tests exercise nested divergent graphs over repeated invocations, independent external
channels through DataFusion UNION, different projection schemas, persistent watermark/checkpoint/
end-input controls, and a multi-megabyte payload with one Flink lease through the last output.
The common Rust execution context now owns either a tree or a region, sharing its Flink memory
pool, task runtime, input cache, state/resource bindings, control events, and metrics. Lifecycle
lookup visits each region definition once. Single-output compatibility APIs reject region plans;
region invocation completion keeps the context busy until every output finishes, and failed
execution requires recovery. Lowering denial can retry without retaining a partial graph.

Generated native window tests compare a shared HOP COUNT region with the existing tree over
three seeds and both backends, including canonical snapshots and cross-backend restore. A mixed
COUNT -> Calc -> attached local MAX region binds global keyed state and the original Flink local
buffer capacity in the same context, and matches the tree's outputs and stage counters through
watermark, pre-checkpoint, and end-input controls. These are native integration prerequisites;
the version-2 JNI region edge now exposes port-tagged Arrow C Data outputs with a separate schema
negotiation for each exit. It reuses the tree edge's C Data input importer, buffer accounting, and
producer-owned release callbacks. An output handle owns the native invocation even if the Java
context handle closes first; exported arrays remain valid after output-handle close. Cancellation,
export failure, and partial import release their native work before Java receives an exception.

An Arrow C Stream has one schema. Flink's reuse before different exchanges requires differently
typed exits from one owner, so this edge returns a port number with a standard Arrow C Data batch.
It does not serialize a batch or introduce another exchange. Generated Java boundary tests check
nullable strings, nested arrays, RowKinds, timestamps, payload buffer identity, schema negotiation,
and release behavior. The Java Arrow region wrapper shares input negotiation/export ownership
with the single-output tree edge, caches each exit schema, and returns port-tagged batches with
owned RowKind/timestamp envelopes. It releases all exits on import failure and permits returned
batches to outlive the input, context, and invocation handles. Empty invocations and repeated
schema negotiation have generated boundary coverage. Edge version 2 adds direct IPC input to
version 1's C Data input/output contract. A network frame can also enter the region
through the shared tree/region IPC decoder: it copies the frame once into a body-aligned native
buffer, decodes there, and keeps that buffer shared through the native graph. Only the final
port-tagged outputs return to Java. Generated boundary checks compare direct IPC with C Data
inputs across alternating external ports, singleton/hash exchanges, RowKinds, timestamps, and
stage counts; failed decode remains retryable and failed output setup cancels the invocation.
Java control propagation can now consume the flat region definition directly: each physical
stage has one control node, external inputs retain their own ports, and shared stages propagate
to each downstream consumer. All exits remain separately identified. The existing scheduler
coalesces each control wave into one native invocation and drains it before forwarding output
watermarks or end-input events; a failed drain forwards neither. Generated control sequences
match real Flink operators at every stage, including idleness. Flink samples one outgoing branch
for latency markers, while it broadcasts watermarks and status. Shared native latency routing is
therefore rejected until it preserves that sampling; shared-plan admission must reject enabled
Flink latency tracking rather than broadcasting those markers. Restored
window-clock clamping reaches every downstream exit. Nested UNION control graphs are rejected
until their physical channels are flattened with Flink's wiring semantics.

Control invocation protocol 2 adds a distinct processing-time timer event. It addresses only the
owning stage and does not advance or forward a watermark. Capability protocol 2 is emitted only
when a bound native factory accepts that event; existing event-time operators continue to expose
protocol 1 and reject processing-time input before state or metric mutation. A version-one message
cannot carry the new event/capability. Java negotiates the capability before dispatch and retains
its fail-closed output-drain lifecycle. The event uses the ordinary native Arrow execution tree,
not an operator-specific JNI path. The region edge now schedules the earliest deadline reported by
negotiated native owners with Flink's processing-time service. It refreshes this bounded descriptor
snapshot only after restore or a fully drained invocation; stages without processing-time capability
make no deadline JNI calls. Cancellation invalidates stale callbacks, timer/output failures require
recovery, and finish cancels outstanding callbacks without firing open windows. Native state owns
absolute timer keys; Flink owns the clock, mailbox callback, and scheduling. Descriptor reads do not
add per-allocation memory reservations.

Capability protocol 3 additionally binds each clock-consuming stage to a distinct external input
port. Native binding validates that the owner directly consumes that edge: clock sampling cannot
move across an intervening native operator. Both tree and shared-region drivers attach one extra
C Data descriptor pair per negotiated clock port to the existing invocation. Logical input schemas
and schema caching stay separate; empty/control inputs have zero-length clock vectors and do not
read the clock. An IPC frame supplies its logical row count through header-only inspection, and
Rust attaches clock metadata after its single payload decode. No payload is reconstructed in Java.

The clock vector uses non-null Arrow Int64 epoch milliseconds (`__streamfusion_processing_time_v1`),
allocated through the Flink-backed edge allocator before sampling one clock value per record.
Clock rollback is preserved. Buffers use the producer's C Data release callbacks and remain under
their original accounting. Rust validates port, shape, length, and nullability before invocation,
and requires the bound consumer to remove clock metadata before output. The new metadata does
not become a SQL column or a downstream clock. Old capability versions cannot advertise clock
ports; existing event-time operators keep their previous descriptor count and capability version.

Tests cover native tree/region consumption, IPC input replacement, signed clock values, shared
buffer identity, producer/import failure cleanup, malformed descriptors, and rejected clock
placement. No production window factory enables this capability yet. Q12 remains gated until its
native processing-time window kernel and complete Flink parity/recovery contracts are implemented.

Shared regions can bind the existing Flink memory/state lifecycle directly. The same backend
leases and checkpoint participants serve tree and shared definitions; they do not create another
budget or state owner. Window union clocks are initialized once per physical definition, retain
the minimum restored subtask clock, and bind that clock before native input can run. Local-window
resource validation also visits each shared definition once and retains its original Flink
capacity through factory serialization. Boundary tests cover failed construction/restore cleanup
on both backends, union-clock restoration, and local capacity binding. The multi-output window
fixture now runs the same generated Flink recovery cases on both exits:
canonical savepoints (including switching backends), aligned and unaligned operator checkpoints,
late input before the first replayed watermark, and rescaling with the minimum union clock.
Both exit changelogs and controls match Flink, with one native window state owner and managed
memory released at teardown. Network-task tests additionally replay input channels through
both exits, including the COUNT-to-local-MAX topology described below.

The Java metric publisher also consumes flat physical definitions directly. It binds each
original stage scope once, omits anonymous local Input slots, and verifies the complete native
snapshot without doubling a reused producer's counts. The runtime owner counts external I/O
separately. The common Flink runtime factory now binds either the original tree or a shared
region to these lifecycle, control, and metric services. One arrival dispatcher drains native
outputs cooperatively: port zero uses the main Arrow output, and other ports use typed Flink
side outputs. Each batch is borrowed synchronously and released after collection, including
when downstream collection fails. The exchange edge feeds its IPC frame directly into the
same shared invocation.

This runtime subset requires one external input, diverging unary stages, disabled latency
tracking, and identical restored window-clock ancestry at every exit. Watermarks and status
are broadcast once after all exits complete the same control wave; incompatible frontiers
fail instead of emitting an incorrect watermark. Generated runtime tests compare both
heterogeneous exit changelogs against Flink-generated Calc operators, with nullable data,
all row kinds, repeated arrivals, direct Arrow and IPC inputs, stage I/O counts, control
broadcasting, and managed-memory release on successful and cancelled execution. These runtime
checks support admission of the verified shape but do not establish production benchmark results.

The selected-graph translator now binds one cached owner to all physical stages in a shared
region. Translating either exit materializes that owner once; the main output and virtual Flink
side-output transformations refer to the same runtime operator. Existing exchanges and key-group
routing remain in place. Every exit records its original Flink resource identity, and factory
serialization retains the completed local-buffer share. Shared contract and runtime-edge
preflight run before graph replacement commits; failure restores retained Flink boundary edges.
Enabled latency tracking is rejected before a shared owner is selected.

Generated complete SQL graphs now compare reused HOP COUNT, attached local/global MAX and binary
MultiJoin outputs against Flink on both backends. The fixtures use the benchmark's MultiJoin
optimizer setting and verify that topology explicitly. Three generated inputs cover 6-second
and 10-second HOP windows and parallelism one and two, with exact external changelog bytes,
nonzero shared native activity and zero retained local-window bridge calls. Topology tests verify
one three-stage owner, two Arrow exits, one keyed-state identity, original resource binding, and
successful JobGraph serialization.

The Flink mailbox-task recovery matrix now exercises both shared exits through separate, real
Arrow-to-RowData sink adapters and network result writers. Two input channels deliver generated
Arrow IPC partials between checkpoint barriers. Both aligned and unaligned checkpoints restore
on memory and RocksDB; each exit's complete changelog, record envelope and watermark sequence
matches the corresponding SQL-generated Flink operators. The variants cover shared HOP COUNT
and attached global MAX/COUNT, both with a following Calc and with a buffered attached local
MAX/COUNT on the second exit. The local stage uses its original 3 MiB buffer capacity and
checkpoint flush callback. Each output delivers exactly one checkpoint barrier. Three generated
inputs include late partials and replayed older watermarks. These tests verify input-channel
replay through the shared topology; they do not measure full-query performance. Ordinary
whole-plan selection now admits the verified divergent streaming region subset.

Comet retains Spark's exchange reuse, while Flink can reuse an intermediate stage before two
different exchanges. This Flink-specific topology keeps the shared computation in one native
owner; it does not duplicate the stage or insert an artificial exchange to avoid native sharing.

The native stream API now provides a bounded fan-out primitive for that integration. It shares
one producer execution and holds at most one batch descriptor while waiting for readers; Arrow
arrays and their existing memory leases remain shared. Consumers must drain cooperatively.
Completion waits for every consumer's EOF. Cancellation, producer failure, or panic releases
pending work and fails the invocation; persistent contexts require recovery before reuse.
Sharing must be bound before the first poll, so an old completed stream cannot release a later
invocation's ownership.

Native tests exercise generated reader schedules, array identity, a single large-buffer Flink
lease through successful and cancelled consumption, and different downstream DataFusion projection
schemas. Control-output tests verify watermark, pre-barrier, and end-input computation and metrics
once per stage, including cancellation and panic cleanup. Fan-out now backs the native region
driver and JNI edge described above. Ordinary planner selection uses this integration for the
verified divergent streaming subset. Q5 release performance validation remains outstanding.
