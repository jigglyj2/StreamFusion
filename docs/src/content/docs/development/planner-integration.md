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
the local physical stage with the ordinary rewrite identity/UID registry. Production admission remains gated.

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

Aggregate, window, and OVER plan builders live in `streamfusion-flink-planner` under its
`planner.aggregate`, `planner.window`, and `planner.over` packages. Their Calcite and Flink planner
dependencies stay inside the isolated planner loader. Runtime operators receive protobuf plans
and runtime types. A source guard rejects new planner dependencies in those runtime families.
The Flink loader patch delegates the `tech.streamfusion` namespace component-first: planner
classes come from the component, while shared runtime, Arrow wrappers, and protobuf classes
come from the runtime owner. This does not expose Calcite through the runtime loader.

Both modules publish normal Maven JARs for compilation and explicit `-bundle.jar` distribution
artifacts. The runtime bundle owns Arrow, native bridges, and protobuf dependencies; the planner
bundle contains only planner classes and relocates protobuf references to match the runtime.
The launcher installs those matching bundles and checks native Calc, shared-plan UNION inputs,
and whole-plan fallback. This packaging check supplements the generated SQL parity tests;
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
runtime's single input. Generated direct and attached HOP SQL tests verify this wiring on both
backends. Reused-output ownership and full-query validation still block ordinary whole-plan Q5
admission; see [Window aggregation](/StreamFusion/operators/window-aggregation/).

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
per-port JVM Arrow C Data routing and full Flink topology/recovery/parity validation remain
outstanding. No additional whole-plan query is admitted by this change.

This layout is currently used for ownership admission. Executing multiple exits still requires
integration of native fan-out, output transport, and corresponding control/metric/recovery handling; the
multi-output fallback gate remains active. Comet retains Spark's exchange reuse, while Flink can
reuse an intermediate stage before two different exchanges. Supporting that Flink topology must
keep the shared computation in one native owner and preserve the existing exchanges; it must not
duplicate the stage or insert an artificial exchange to avoid native sharing.

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
once per stage, including cancellation and panic cleanup. This primitive is not yet wired to the
multi-output planner or JNI output routing. It does not change ordinary Q5 admission or establish
full-query recovery/performance results.
