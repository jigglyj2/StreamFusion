---
title: Memory and configuration
description: Comet-style native reservations governed by Flink's existing resource model.
---

StreamFusion follows DataFusion Comet's reservation model. Flink owns resource allocation;
Rust and Arrow allocate physical storage. StreamFusion does not override Rust's global
allocator to call Flink for each allocation.

## What is accounted

Reservations cover large Arrow buffers, growing DataFusion structures, retained native state,
and large temporary workspaces. Credit follows the buffer or state owner until its last
reference is released, including across Arrow C Data/C Stream handoff and cancellation.
Shared allocations count once, even when an IPC payload backs many columns or slices.
Imported Java buffers retain their producer's budget and release callback; native forwarding
does not charge that payload again. Newly allocated native output retains its native reservation.

Small bounded temporaries, execution-stream wrappers, Arrow descriptors, and control objects
do not need individual reservations. Allocation instrumentation can diagnose unexpected growth;
matching every ephemeral allocation to a reservation is not a production admission requirement.
This exception does not permit unbounded temporary allocations or unaccounted retained state.

Use DataFusion's memory consumers and reservation pool for DataFusion operators. Custom native
state uses coarse reservations on the same Flink broker. Ordinary buffer ownership and Arrow
release callbacks remain responsible for physical lifetime. Compatibility copies, such as a
rebased validity bitmap at the C Data boundary, remain budgeted.

Filters evaluate their predicate once. All-pass output retains the input buffers; all-rejected
input needs no gathered payload. Partial selections reserve gather space from the projected
logical buffer spans, avoiding multiplication of a shared IPC allocation by the schema width.
Operations that expand output, such as `REPEAT`, must reserve their large output before allocation.

## Flink budgets and settings

The TaskManager's `taskmanager.memory.managed.size` or
`taskmanager.memory.managed.fraction`, together with Flink's consumer weights, determines the
available budget. Native DataFusion and Arrow Java participate in that resource model.
`taskmanager.memory.task.off-heap.size` is not an additional untracked StreamFusion allowance.
No separate StreamFusion memory budget or admission bypass is supported.

Fused regions preserve the managed-memory weights of their state owners. A RocksDB cache and
write-buffer manager are shared by the owners of the same Flink shared memory resource.
Different resources with equal byte limits must remain independent. The reservation is released
only after the last owner closes; restore readers without a shared resource use isolated pools.

Flink's configured incremental-checkpoint setting determines whether the native RocksDB adapter
uses incremental handles. Selecting RocksDB does not implicitly enable incremental checkpoints.
Other RocksDB settings still require equivalent native handling before production stateful
admission. Unsupported configuration or uncertain semantics require whole-plan Flink fallback.

The runtime configuration surface follows Flink. StreamFusion-specific runtime options are
limited to enabling acceleration and explicit opt-ins for operators whose behavior differs from
Flink. Implementation constants and benchmark controls must not become deployment tuning knobs.
Native connectors must map each applicable Flink/client setting or retain the Flink boundary
with a precise fallback reason.

## Current readiness

The verified in-memory binary equi-join path is admitted. Other persistent stateful families
and RocksDB remain gated by
[architecture admission](/StreamFusion/development/architecture-admission/). Their large-state
memory behavior, backend configuration, metrics, and checkpoint/restore contracts must be verified
before admission. Removing descriptor reservations does not establish those contracts.

Native parity tests must require acceleration and native execution. Tests exercising whole-plan
fallback are explicitly identified as fallback coverage; a successful Flink-versus-Flink comparison
does not establish native operator parity.

## Local window flush capacity

The buffered local-window kernel accepts Flink-resolved capacity through `NativeTaskBindings`
version 1. Each `NativeLocalWindowBuffer` supplies the original physical stage's resolved managed
memory share and Flink page size. These values model `WindowBytesMultiMap` flush boundaries;
they do not reserve raw Flink rows or grant another native memory budget. DataFusion group
vectors, retained Arrow row keys, input cursors and output buffers continue to consume the normal
host pool through coarse reservations.

The shared runtime factory carries a serializable map from each local stage's stable identity to
its original Flink operator weight, total slot-group weight and managed-memory use cases. At task
initialization it applies Flink's fraction rounding, configuration precedence, backend managed-memory
flag and page sizing to that original share. The native runtime's own fraction remains unchanged;
adding a native state owner or Arrow adapter must not alter the modeled local flush capacity.
Missing, extra or duplicate local owners are rejected before task startup. A resolved zero capacity
fails before native allocation.

The task binding is installed once, before native lowering, alongside any separate keyed-state
bindings. Invalid requests leave existing bindings unchanged and return their temporary credit.
The Java/native constructor admits all protobuf copies before JNI. Original-plan resource-share
resolution in ordinary planner translation is still required before admitting the Q5 window plan;
runtime binding/parity tests are not production Nexmark admission. Generated runtime tests compare
pressure flushes and control changelogs with Flink using a 3 MiB original capacity and a larger
native allowance, both for a local stage alone and for a local/global tree on both backends.

The planner now has an original-resource graph calculator, verified against Flink-generated
job graphs. It snapshots physical edges before replacement, counts reused stages once, and
uses retained boundary transformations for their actual resource declarations. It takes the
complete pipeline, including DataStream operators added outside the SQL graph, and preserves
slot-group inheritance and managed-memory use-case membership. Replacement-operator weights
cannot change the saved original geometry. The verified internal resource contracts cover
streaming local/global windows, grouping aggregates, joins, and their stateless/wiring stages;
this resource support does not establish operator acceleration support.

Standard physical boundary transformations and non-committing Sink V2 writers are understood.
Arbitrary sink pre-write/commit expansions and unknown transformation resource contracts are
rejected by the calculator without invoking connector topology construction. Eight generated
job-graph cases cover reuse, weighted sources, unions across distinct slot groups, downstream
weights added after SQL translation, and legacy/Sink V2 boundaries. These comparisons verify
Flink's final managed-memory fractions and resolved byte capacities.

The patched Flink pipeline generator now invokes a runtime-side finalizer after the complete
pipeline is available and before JobGraph serialization. Native local-window factories can carry
a planner-only resolver until that point. The finalizer resolves each shared planner once per
pipeline and installs resolved factory copies in the generated graph; the reusable transformations
remain unchanged. Reusing a planned stream with different downstream weights therefore does not
change a previously generated job's capacity. All owners resolve before any copy is published.
Unresolved local resources cannot be serialized or opened in a task.

This callback lives in the runtime-side planner factory so it does not require access to Flink's
isolated planner classloader. The distribution runner patches `StreamGraphGenerator` in the Flink
distribution JAR as well as the existing planner/API hooks. Ordinary local-window exec-node
selection still needs to attach the original-resource calculator to this resolver before Q5 can
be admitted.
