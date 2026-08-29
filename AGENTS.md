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

Nexmark is an opt-in benchmark, not part of routine push or pull-request CI. Run it
deliberately with `mvn -pl streamfusion-nexmark-benchmarks -am
-Pbenchmark-integration verify` when validating benchmark or integration changes.
Do not add it to the normal CI workflow unless this policy is explicitly changed.

Immediately before each commit, run Palantir Java formatting and only the unit tests
relevant to that commit. Treat this like a focused commit hook; do not spend time
running unrelated test suites.
