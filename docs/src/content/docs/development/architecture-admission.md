---
title: Architecture admission
description: Production selection, shared native execution, and remaining whole-plan fallback requirements.
---

An implemented native kernel is not automatically eligible for production selection. The
architecture admission pass checks the original physical graph before replacement. A rejected
node keeps **every root and every operator on Flink**. EXPLAIN identifies each blocked operator
and unsupported native connection. There is no admission bypass setting.

## Architectural reference

DataFusion Comet is the reference for protobuf plan lowering, a shared native execution tree,
Arrow buffer ownership, and per-stage metric propagation. StreamFusion's intentional difference
is whole-plan fallback. Flink also retains its streaming control plane: watermarks, mailbox
ordering, state backends, checkpointing, recovery, and distribution.

Adjacent native operators pass reference-counted Arrow batches directly. Each vectorized
operator finishes its batch operation before its consumer proceeds. Fusion does not mean
concurrent buffer mutation or replacing independent DataFusion stages with a bespoke kernel.
Every stage retains its protobuf identity and independently observable metrics.

Memory admission follows Comet's large-owner reservation model. It covers Arrow payloads,
materializing workspaces, retained state, and RocksDB resources. Small bounded descriptors and
short-lived control objects do not need individual reservations or JNI callbacks. An exhaustive
Rust heap census is neither an admission condition nor a production allocator design. See
[Memory and configuration](/StreamFusion/development/memory-and-configuration/) for ownership,
budget, and backend configuration constraints.

## Current selection gates

Calc, UNION ALL and verified binary inner equi MultiJoin compose through the common native region.
That join subset is admitted with in-memory or default RocksDB state; other persistent operator families
remain on whole-plan Flink fallback until their large state/buffer admission, backend settings,
checkpoint behavior, and complete Flink metric contracts are verified through that region.
Some retained implementations, including deduplication and aggregation, already have direct
shared-plan conformance coverage; that does not remove the production gate.

Other implemented families also require general region composition and per-stage metric parity.
Shared internal stages with multiple consumers are rejected until native multi-output ownership
can preserve one execution and one metric identity. Unsupported schemas, settings, and semantic
subsets retain their precise fallback reasons. Sources and sinks may use explicit Arrow/RowData
edge adapters; an internal RowData operator or an intermediate JNI round trip is not admitted.

A binary Flink `StreamExecMultiJoin` with a common equi key lowers to the regular join algorithm.
Its shared-region composition is admitted after generated metric/changelog comparisons, keyed
rescaling and channel replay tests. Persistent state is admitted when its complete
condition is covered by the common equality keys; additional residuals retain the state gate.
A separate backend guard checks the packaged RocksDB library's CPU compatibility and checksum.
TaskManager log relocation matches Flink. Unsupported typed RocksDB settings still report their
specific option first.
Checkpointing during channel recovery remains unsupported on either backend. Genuine multi-way joins have paged state
and a bounded output cursor in their retained native implementation, but still require integration
with the common ExecutionPlan, per-stage metrics, and checkpoint/control lifecycle. Their gate
must describe this missing integration rather than claiming whole-key rewrites remain implemented.

## Batch and control boundaries

The shared context caches the lowered DataFusion tree. Synchronous single-input Calc/filter
chains also retain their execution stream across mailbox arrivals: temporary lack of input means
Pending internally, while each Java invocation drains only its own arrival. A test feeds twenty
arrivals through one stream and checks output and stage counters.

Multi-input and stateful paths still construct an invocation stream at their explicit Flink
batch/control boundaries. This is a constrained streaming-lifecycle difference from Comet, not a
second operator architecture. General reuse requires proof that Pending, end-of-input, watermarks,
and checkpoint flushing retain their distinct meanings for each physical stage.

After input schemas are negotiated, inactive multi-input ports use native empty placeholders.
They do not repeatedly export/import empty Arrow arrays through Java. Network exchange receivers
send the existing IPC payload range and routing metadata directly to the native plan edge. Rust
decodes once and builds the owned record envelope there, without importing the decoded payload
into Arrow Java and exporting it again. Flink continues to own network transport and routing.

C Data/C Stream release callbacks retain payload leases until the final consumer releases them,
including output held after context close. Compatibility copies remain admitted when Java cannot
represent a sliced buffer safely. Descriptor-only exports do not require new payload credit.

Control requests address stable stage IDs for watermarks, pre-checkpoint flush, and end-of-input.
Ordinary invocation EOF does not substitute for a control event. Unknown stages, invalid bindings,
and unsupported protocol versions fail closed. Plan protocol 3, control-edge API 2, and gauge-edge
API 1 are required; the state plugin ABI is version 8, including the Flink resource-scope identity, bounded ordered
ranges and task-resolved RocksDB log directory.

## Evidence and next milestone

Native tests cover buffer sharing, denial and cleanup, retained outputs, stream reuse, state
bindings, dirty-page writes, bounded fan-out, and canonical memory/RocksDB restore. Generated
Flink comparisons and Java harnesses cover the corresponding changelog and boundary contracts.
These checks are correctness evidence; no throughput improvement is claimed without a release
benchmark and representative mixed JVM/native profiles.

SQL native-parity assertions require `Accelerated: yes` and the expected native activity. Explicit
fallback-parity tests require whole-plan fallback and zero native batches. A Flink-versus-Flink
comparison cannot count as evidence that a native operator works.

Production milestones follow increasing Nexmark query order. Verify Q0–Q2, then complete Q3's
regular streaming join on both backends, with generated output/metric parity, recovery coverage,
and release comparison against Flink before advancing. See
[Query checkpoints](/StreamFusion/benchmarks/query-checkpoints/). Test-only graph conversion and
historical benchmark results do not establish current production admission.
