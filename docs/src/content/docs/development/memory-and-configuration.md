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

The task binding is installed once, before native lowering, alongside any separate keyed-state
bindings. Invalid requests leave existing bindings unchanged and return their temporary credit.
The Java/native constructor admits all protobuf copies before JNI. Original-plan resource-share
resolution in ordinary planner translation is still required before admitting the Q5 window plan;
manual binding/parity tests are not production Nexmark admission.
