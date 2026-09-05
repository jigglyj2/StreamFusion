# Repository Guidelines

## Project Overview

We are creating a Flink accelerator on top of Apache DataFusion. This means we'll use DataFusion to accelerate operators where possible, otherwise we'll create our own based on Arroyo and RisingWave code. The rust layer is just responsible for execution, the existing Flink code is responsible for snapshotting, checkpointing, distribution, recovery, and planning. If you need to reference external code, check if it is in ~/data, and if not, clone it there.

## Project Structure & Module Organization

Structure this project like Flink and use Flink's own module boundaries as the default model.
Core planner and runtime machinery such as exchange belongs with the corresponding core
StreamFusion planner/runtime package, just as it does in Flink; do not create a standalone Maven
module merely to isolate a core subsystem. Use separate optional Maven modules for genuinely
optional extension points such as connectors, formats, and independently packaged integrations.

## Build, Test, and Development Commands

Add once configured.

Use the Rust development profile for ordinary implementation iteration and focused
tests when optimization does not affect the behavior under test. Release JARs,
distribution artifacts, and every benchmark measurement must compile Rust with the
release profile; never publish or compare benchmark results from a development build.

Release and benchmark native builds should enable the newest CPU instruction sets
available on their declared target (for local benchmarking, use the build machine's
native CPU feature set). Published artifacts must record and enforce their CPU baseline,
and architecture-specific binaries must be packaged separately so an optimized binary
is never silently loaded on an incompatible host.

## Coding Style & Naming Conventions

Add once configured. Use palantir java format.

Use `tech.streamfusion` for Java packages and Maven coordinates. Keep commits small and
logically focused so each change can be reviewed and reverted independently.

Do not add to or modify `README.md` for now. Keep it empty until this instruction is removed.

The Starlight site under `docs/` is the canonical user-facing documentation. Update it
in the same change whenever behavior, configuration, compatibility, acceleration
coverage, fallback conditions, or other user-visible functionality changes. Keep the
operator status pages explicit about current support; do not document planned work as
implemented.

StreamFusion plan replacement is all-or-nothing. Accelerate a plan only when every
internal node has a StreamFusion physical operator. Sources and sinks are the only
exceptions and must use a StreamFusion connector or a lightweight Flink RowData Arrow
batch view. EXPLAIN output must state why the whole plan fell back and give a reason for
every operator that prevented acceleration.

Fuse adjacent Rust operators into one native DataFusion execution-plan tree. They must
exchange Arrow `RecordBatch` streams directly using shared, reference-counted buffers;
the handoff between adjacent native operators must not serialize or copy whole batches.
An operator may allocate output buffers when its computation inherently requires them.
Cross the JVM/native boundary only at the edges of the fused native plan.

"Fused" means one native plan with zero-copy handoff, not kernel fusion or concurrent
mutation of a batch. Follow Comet's vectorized execution model: each DataFusion physical
operator consumes an Arrow batch and completes its operation before the next operator
consumes the resulting batch (for example, calc -> calc -> calc). Keep operator stages
independently observable and testable.

Every StreamFusion runtime operator must use an Arrow batch as its Flink data-plane input
and output type. The only RowData operators allowed in an accelerated topology are the
explicit RowData-to-Arrow adapter immediately after a non-native source and the explicit
Arrow-to-RowData view adapter immediately before a non-native sink. Java-side control
operators such as watermark assignment may inspect zero-copy RowData views over an Arrow
batch, but must still accept and emit the Arrow batch and must never transpose, buffer,
or reconstruct its payload as rows. VALUES emits Arrow directly; UNION ALL forwards Arrow
batches; changelog filters select Arrow ranges; and exchange adds/removes only its metadata
vectors around Arrow IPC. Add a topology/source guard test whenever operator plumbing changes
so a RowData-shaped internal operator or operator-local transpose cannot regress silently.

## Intermediate Operator Checklist

Every intermediate StreamFusion operator (that is, every operator other than a source or
sink) must satisfy all of the following before its functionality is considered accelerated:

1. Accept Arrow batches as input and emit Arrow batches as output. Do not introduce a
   RowData-shaped internal data path.
2. Account for every native allocation, including DataFusion and custom Rust state, through
   Flink's managed/off-heap memory accounting and allocator budget.
3. Expose the same metric surface and semantics as the corresponding Flink operator,
   including operator-specific metrics and logical-record I/O counters.
4. Be directly composable with adjacent native intermediate operators inside one fused
   execution-plan tree. Invoking it inline must not send an Arrow batch through JNI or Java
   between operators.
5. If stateful, batch state access: load or access all required RocksDB keys at the beginning
   of each incoming batch, perform the batch computation, and flush all dirty state at the
   end of that batch. Do not perform per-row RocksDB round trips.
6. Use vectorized execution wherever possible and DataFusion operators, expressions, and
   kernels wherever they preserve Flink semantics.
7. If stateful, support both the RocksDB and in-memory state backends and preserve correctness
   under aligned and unaligned checkpoints, including restore and rescaling where applicable.
8. If any operator feature or semantic subset is too complex or its Flink parity is uncertain,
   reject acceleration for that case during planning, report the precise fallback reason, and
   run the complete plan with the normal Flink operator. Defer that subset rather than
   approximating its behavior.
9. Fully document supported semantics, limitations, metrics, memory behavior, state/backend
   behavior, and fallback conditions. Update the canonical `docs/` pages in the same change
   whenever any of these change.

## Source and Sink Operator Checklist

Every StreamFusion source or sink implementation must satisfy all of the following:

1. Minimize the surface area replaced by native code and focus acceleration on the critical
   data path. Keep Flink's existing control-plane behavior wherever it does not need to move
   native. For example, an Iceberg sink may write data files natively while retaining Flink's
   commit coordination, and a Kafka source may read and decode data natively while retaining
   broker coordination and transaction handling in Java.
2. Expose the same metric surface, types, units, scopes, lifecycle, and update semantics as the
   corresponding Flink source or sink.
3. Respect every relevant configuration supported by the existing Flink source or sink. Map
   each setting to equivalent native behavior where native code owns that concern; if exact
   semantics cannot be preserved, retain the Flink implementation and report the fallback
   reason rather than silently ignoring the setting or using a native default.
4. Import the applicable upstream Flink SQL tests for the source or sink, run them against
   StreamFusion acceleration, and make them part of normal CI. Add StreamFusion-owned parity
   coverage for behavior that upstream tests do not exercise.

Build the native physical-plan and expression tree on the Java planner side and encode
it as a versioned Protocol Buffers contract for Rust to decode and lower to DataFusion,
following Comet's operator protobuf model. Protobuf is the control/plan format; Arrow C
Data/C Stream remains the batch transport. Reject unknown or semantically unsupported
messages with an EXPLAIN fallback reason rather than approximating Flink behavior.

Treat DataFusion Comet as the primary architectural reference for planner work and
communication between JVM operators and native operators. Follow its model of planner
rules selecting distinct accelerator exec nodes while leaving the engine's original
nodes available for fallback; do not add acceleration branches inside Flink operators.
Also follow Comet's protobuf plan communication, Arrow batch transport, ownership, and
metric propagation patterns unless Flink semantics require a documented difference.

Preserve Flink metric compatibility for every accelerated physical operator. Publish
the same Flink metric names, types, units, scopes, lifecycle, and update semantics as
the Flink operator being replaced, and make deterministic values match for identical
inputs, control events, checkpoints, and terminal paths. Runtime-dependent timings,
rates, and physical byte counts must retain Flink's definitions while measuring the
actual accelerated execution; never forge them to resemble a Flink run. Standard I/O counters must count
logical Flink records, never internal Arrow batches or IPC frames. Preserve every
operator-specific counter, meter, histogram, and gauge; if exact semantics cannot be
represented, keep the operator on Flink and report the fallback reason. For fused
native plans, carry stable plan-node identities through protobuf and propagate native
metrics back to the corresponding Java physical nodes using Comet's metric-tree model
so each stage remains independently observable. StreamFusion-only diagnostics must
live in an explicitly named StreamFusion metric subgroup and must not replace or
redefine a Flink metric. Generated parity tests must compare the complete Flink metric
surface, deterministic values, and runtime-dependent metric semantics in addition to
the output changelog.

Account all StreamFusion native memory, including DataFusion and custom Rust data
structures, through Flink's existing managed/off-heap memory model. StreamFusion must
not introduce a separate deployment-time memory budget: existing Flink TaskManager
managed-memory size/fraction and consumer-weight settings govern its allocation. Do
not add StreamFusion deployment toggles when an equivalent Flink setting exists. A
StreamFusion-specific feature gate is acceptable only when users must explicitly opt
into behavior that may not be byte-identical to Flink.

When implementing a native source or sink, translate every relevant Java/Flink
connector and client setting to the native library's equivalent. If a setting or its
semantics cannot be represented exactly, keep that boundary on the Flink implementation
and report the fallback reason; never silently use a native-client default.

Keep an operator's or connector's Rust implementation in the optional Maven module
that owns that integration, including its platform-specific native artifacts. Do not
grow a central native crate with optional integration code. Load independently packaged
native components through a versioned C ABI modeled on ADBC: a stable initialization
entry point negotiates an ABI version and returns a function table with opaque handles.
Exchange columnar data between native components directly through the Arrow C Data and
C Stream interfaces, including their release callbacks; do not route native-to-native
batches through JNI or Java. Never expose Rust's unstable ABI across module boundaries.

Give every native operator its own Rust source file. When an operator becomes complex,
split expression conversion, state, algorithms, or other coherent responsibilities into
an operator-specific submodule; do not accumulate unrelated operators or a large
monolithic planner implementation in one file.

Keep Java and Rust source and test files focused on one coherent responsibility. Split
operator families, expression families, protocol fixtures, and shared test machinery
into appropriately named files before they become megafiles; do not grow catch-all
translator, bridge, or parity-test classes.

Use the Arrow C Data and C Stream interfaces for production Java/Rust batch transfer,
with explicit producer-owned release callbacks. Treat sliced arrays as a compatibility
boundary: before exposing Rust-produced slices to Arrow Java or a RowData view, normalize
them to a zero-based Java-safe representation and rebase variable-width or nested offsets
when required. Preserve zero-copy buffers when Java can represent the slice correctly;
copy only the buffers whose offsets/alignment cannot be represented safely. Add boundary
tests for sliced fixed-width, variable-width, nested, nullable, dictionary, and decimal
vectors, including non-zero offsets and exactly-once release behavior.

## Testing Guidelines

Exact byte to byte parity with Flink's result set is paramount. Add our own tests to ensure this, use normal Flink processing when we can't achieve it. Hook into existing Flink SQL targets where possible.

Every operator, expression, connector, and execution-boundary implementation must add
StreamFusion-owned generated parity tests that run identical inputs through Flink and
StreamFusion and compare the complete changelog byte-for-byte. Also run the corresponding
Flink SQL harness tests wherever they can exercise the changed behavior. Generated parity
tests supplement upstream Flink coverage; neither test layer replaces the other.

The Kafka-in/Kafka-out, exactly-once Nexmark comparison against unmodified Flink is
our north-star benchmark. Optimize its four state-backend/mini-batch cases while
keeping the code simple and avoiding substantial divergence from Flink's result
parity and architecture.

Benchmark-driven optimizations must preserve the intended StreamFusion architecture;
never trade away the production design merely to improve a benchmark result. Before
adopting an optimization, evaluate whether the equivalent change would make architectural
sense in DataFusion Comet's vectorized execution model. Reject benchmark-specific shortcuts,
RowData processing inside native operators, extra JVM/native crossings, whole-batch copies,
disabled Flink semantics, or special cases that Comet would reasonably consider an
architectural regression. Treat a faster result as actionable only when the optimized path
remains representative of the production Arrow and DataFusion execution path.

Nexmark is an opt-in benchmark, not part of routine push or pull-request CI. Run it
deliberately with `mvn -pl streamfusion-nexmark-benchmarks -am
-Pbenchmark-integration verify` when validating benchmark or integration changes.
Do not add it to the normal CI workflow unless this policy is explicitly changed.

For performance investigations, build Rust with `--release`, the benchmark machine's
native CPU features, frame pointers, and profiling symbols; profiling symbols must not
reduce the release optimization level. Measure Flink and StreamFusion in separate JVMs
with identical heap, parallelism, checkpoint, source, sink, and event-count settings.
Use at least three unprofiled measured forks, alternate engine order, and report the
median plus dispersion. State whether the timing is end-to-end or steady-state, record
the commit and machine/runtime details, and verify that StreamFusion EXPLAIN reports
acceleration and that native batch counters are non-zero. Never use profiler-instrumented
throughput as the benchmark result.

Capture mixed JVM/native CPU profiles on a longer representative fork of every compared
query. On Linux, use async-profiler (or an equivalent sampling profiler) with Java
non-safepoint sampling, native DWARF/frame-pointer unwinding, and JFR output, then retain
the per-engine flame graphs, collapsed stacks, and a differential flame graph. Inspect
the complete source -> RowData/Arrow boundary -> JNI -> Rust/DataFusion -> Arrow/RowData
boundary -> sink path, not an isolated native microbenchmark. Report native invocation
counts and inclusive CPU shares for row copying, RowData-to-Arrow transposition, Arrow C
Data/JNI transport, native plan lowering, DataFusion execution, and Arrow-backed output
access. Keep generated profiles under the benchmark module's `target/` directory unless
the user explicitly requests checked-in artifacts.

Immediately before each commit, run Palantir Java formatting and only the unit tests
relevant to that commit. Treat this like a focused commit hook; do not spend time
running unrelated test suites.
