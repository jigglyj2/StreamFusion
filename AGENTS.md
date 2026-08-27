# Repository Guidelines

## Project Overview

We are creating a Flink accelerator on top of Apache DataFusion. This means we'll use DataFusion to accelerate operators where possible, otherwise we'll create our own based on Arroyo and RisingWave code. The rust layer is just responsible for execution, the existing Flink code is responsible for snapshotting, checkpointing, distribution, recovery, and planning. If you need to reference external code, check if it is in ~/data, and if not, clone it there.

## Project Structure & Module Organization

Structure this project like Flink. We should have different optional maven modules for different extension points.

## Build, Test, and Development Commands

Add once configured.

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

## Testing Guidelines

Exact byte to byte parity with Flink's result set is paramount. Add our own tests to ensure this, use normal Flink processing when we can't achieve it. Hook into existing Flink SQL targets where possible.

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
