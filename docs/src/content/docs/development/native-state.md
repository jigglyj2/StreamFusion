---
title: Native keyed state
description: Backend contract, checkpoint formats, and implementation references for native operators.
---

The binary inner equi `StreamExecMultiJoin` path is admitted with in-memory and default RocksDB state under
[architecture admission](/StreamFusion/development/architecture-admission/). Other persistent
families retain whole-plan Flink fallback. The implementation details
below distinguish this verified path from retained code and its target memory contract.

Native keyed-region preflight now rejects non-default Flink RocksDB options that the component
does not propagate, including custom option factories, local directories, memory ratios,
fixed or unmanaged budgets, compression settings, and checkpoint transfer thread counts. It
resolves Flink's typed options and aliases before graph replacement; it does not silently use
native defaults. Ordinary Flink managed-memory size/consumer weights and incremental-checkpoint
selection remain configurable. RocksDB-specific options do not reject an in-memory backend.
This configuration guard supplements the existing metric and physical-family admission gates;
default settings have separate runtime conformance coverage. A component guard checks the
packaged RocksDB library's CPU compatibility and checksum before admission. The guard resolves
the user's original backend even after the internal wrapper is installed, so repeated planning
does not mistake that wrapper for a new backend. Checkpointing during channel recovery remains
unsupported; ordinary aligned/unaligned recovery uses the tested non-overlapping lifecycle.

The native RocksDB component now builds and explicitly selects Snappy compression on every SST
level, matching Flink's default compression choice. Previously the codec was omitted from the
native dependency build. A checkpoint test verifies actual compression of repetitive state and
exact value recovery after reopening the saved SSTs. This is a format/configuration check, not
a throughput result; non-default compression settings still require whole-plan fallback.

Opening the native database now applies the configured options explicitly to its default column
family. The previous named-family open helper silently substituted RocksDB defaults there, leaving
the reserved shared-cache wrapper disconnected from the active table cache. A regression checks
the cache capacity reported by each actual database, in addition to shared resource identity.
This restores the intended cache, index/filter accounting, compression and memtable configuration;
it does not complete the remaining Flink-default configuration audit.

Database defaults now also match Flink's background-job count, open-file limit, log level and
rotation limits, disabled statistics dumps, level-compaction sizing and periodic-compaction
interval. Closing the native database skips the redundant memtable flush, as Flink does with
WAL-disabled state: checkpoints establish durability. Tests inspect the opened database's
persisted OPTIONS file and verify both the absence of shutdown-created SSTs and successful
restore from a physical checkpoint.

The shared pools now use Flink's default sizing formula: five sixths of the assigned lease
for the LRU cache and one third for the write-buffer manager, which charges its entries to
that cache. The remaining one sixth allows for the write-buffer manager's over-capacity
threshold. Each column family retains Flink's 64 MiB write-buffer setting and two-buffer limit;
shared memory pressure can trigger flushing earlier. Index/filter blocks use the same cache,
with a 0.1 high-priority pool, matching Flink's default. Tests check actual database cache
capacity, the write-buffer manager and the cache description emitted by RocksDB.

Rust/RocksDB 0.25.0 does not expose the priority ratio through its C/Rust API. The optional
state module carries a small documented binding extension, with the unmodified RocksDB 11.8.1
and Snappy sources pinned as Git submodules. Initialize them with
`git submodule update --init`; routine CI does this at checkout. The extension adds a safe
validated setter and preserves upstream shared cache/WBM ownership. It does not change
RocksDB's execution algorithms. The retained patch and source provenance are in
`streamfusion-state-rocksdb/vendor/rocksdb/VENDOR.md`.
This setter and its minimal binding/build plumbing are an explicit exception to the
repository's no-private-upstream-modifications rule. The exception preserves upstream
cache/write-buffer-manager ownership and does not permit storage-algorithm changes or
unrelated binding extensions. Remove it once upstream bindings provide the equivalent setter.

Default log relocation is now resolved in the TaskManager JVM using Flink's `log.file`
property, readable-file checks and database-path length limit. Tests compare this resolution
with Flink's actual `RocksDBResourceContainer`. The resolved path crosses state-binding protocol
2 and state-component ABI 8; legacy bindings without relocation still use protocol 1. Both
libraries must implement ABI 8. Closing a database removes only its current and rotated log
files after RocksDB closes its logger. Unlike Flink's broad prefix cleanup, neighboring database
names are preserved; this narrows cleanup ownership without changing execution or checkpoint
semantics. Cross-backend lifecycle tests exercise relocated logs and exact state restoration.
Default RocksDB configuration is now admitted for the verified binary inner equi-join subset.
Packaged libraries enforce their CPU requirements; see [Native modules and ABI](/StreamFusion/development/native-modules/).

## Shared native-plan state bindings

The common Java `NativeExecutionContext` accepts a separate, versioned `NativeStateBindings`
protobuf. Flink supplies each physical node's stable identity, maximum parallelism, assigned
key-group range, and memory or RocksDB resources. RocksDB paths and memory leases are task-local
Flink assignments, not deployment settings. The runtime validates the complete binding request
before constructing state, then installs the factories into one native execution context. It
does not create a second execution context for each state owner.

The common keyed-region transformation sums the relative managed-memory weights of its persistent
owners (currently 8 per shared streaming owner). Fusion therefore preserves their combined weight
in Flink's allocation calculation, independently of the number of external inputs. The runtime
still uses one Flink-assigned allowance and one shared RocksDB memory lease.

Deduplication, regular-join, streaming raw/global group-aggregate, and the verified global HOP
slice constructor implement this shared contract. A
common constructor configures the backend once and passes it to each family, which provides
snapshot/restore/checkpoint methods; recursive
physical lowering, Arrow stream execution, and node-addressed control dispatch are shared.
Canonical snapshots remain independently addressable by `(plan_node_id, key_group)` so merging
native regions does not merge their SQL state namespaces. Snapshot and restore cannot race an
active stream. A failed mutating restore requires a fresh context and recovery rather than reuse
of possibly partial state.

The HOP window binding currently accepts append-only UTC event-time partials with DataFusion
COUNT or compatible append-only extrema. It retains base slices for shared HOP windows and exact
fixed-size namespaces for attached HOP windows; attached windows expire once without extra timers.
The snapshot fingerprint pins this distinction. It uses ordered in-memory state or the configured RocksDB
backend, emits through the common unary stream, and snapshots its timer index at Flink's checkpoint
boundary. State-binding protocol 3 adds an optional restored operator watermark for this family.
The shared Java region stores each window clock in Flink union operator state, namespaced by
its stable plan-node ID because a fused region has one Flink lifecycle owner. It supplies the
minimum restored subtask watermark before native context construction and keyed state import;
the clock applies even if no keyed entries need importing. Successful watermark drains update
that operator state for the next snapshot. Replayed older watermarks forward the restored window
clock, while input gauges retain the arrival value. A window restore without this clock is
rejected, as is attaching it to another operator family or an older binding protocol. Existing
protocols 1 and 2 remain supported. Direct generated tests compare the shared HOP COUNT tree with
Flink's SQL-generated global slicer and the attached MAX/COUNT tree with Flink's attached stage on
both backends, including restored input and logical stage I/O.
Runtime tests cover canonical backend switching, aligned/unaligned operator snapshots, generated
replay and Flink union-state 2→1 rescaling. The shared scalar metric channel also publishes real
late-drop counters/meters and a live Flink-clock watermark-latency gauge, with generated default
metric-surface parity. A shared/attached window mailbox-task matrix uses two Flink input channels,
aligned/unaligned barriers, generated IPC frames captured by the channel-state writer, and replay
through Flink's channel-state reader. It verifies exact post-restore rows and watermarks on both
backends, including late partials and older replayed watermarks. Final selected-topology metrics
and recovery, original local memory-share binding and ordinary planner admission remain outstanding; see [Window aggregation](/StreamFusion/operators/window-aggregation/).

The group-aggregate binding accepts synchronous and mini-batch streaming raw input, including
retractions, and mini-batch global partial-accumulator input. Raw/global bundles drain through the
shared child-before-parent control stream; keyed snapshots reject pending or incomplete drains.
Bounded-final mode is still rejected before opening a database. A generated Calc/Aggregate/Calc
comparison uses Flink's SQL-generated aggregate handler and checks complete changelog bytes,
nullable values/keys, all four RowKinds, record timestamps, logical stage counts, and canonical
memory-to-RocksDB and RocksDB-to-memory restore. Further shared Flink runtime tests cover canonical
cross-backend and aligned/unaligned operator-state restoration with the same SQL-handler parity,
and incremental RocksDB SST reuse after checkpoint completion. They verify all managed native
reservations are released at runtime close. This does not establish production planner admission,
full metric-surface parity or in-flight unaligned network replay. A separate aggregation 1→2→1
rescaling matrix now covers all 16 test key groups using Rust hash-exchange frames and Flink's
state repartitioning, with per-key SQL changelog/timestamp parity after restore and retractions.
Canonical restore switches memory/RocksDB in both directions; aligned/unaligned operator-state
restore retains the backend, including physical incremental RocksDB checkpoint handles.

The shared binary join also has a Flink mailbox-task recovery test on both backends. A checkpoint
barrier arrives on one input, then an Arrow IPC frame arrives on the other before its barrier.
Aligned restore retains the joined state; unaligned restore replays the captured frame exactly once
through Flink's sequential channel-state reader and the native exchange edge. Both paths compare
subsequent retractions byte-for-byte with Flink's generated join/Calc operators. The test uses real
checkpoint handles, channel-state writer/reader, recovered input channels, and the production sink
view adapter. The mailbox harness requires explicit insertion of the serialized frame into the
channel-state writer because its test input queues do not implement network capture. This establishes
that recovery boundary, not full distributed failure/restart or in-flight rescaling coverage, and
does not remove other physical families' or RocksDB's remaining production admission requirements.

Task-lifetime lifecycle registration is distinct from keyed state binding. Local aggregate buffers
are discovered from the native plan before capability negotiation and use the same stream/control,
gauge and invocation interfaces. Later keyed bindings merge with these owners and reject duplicate
identities rather than replacing them. The local factory has no backend or keyed snapshot format:
it drains before checkpoints and is reconstructed through replay. Native local/Calc/global tests
cover both global backends and canonical restore with retractions. The local Java fragment now uses
the common one-input owner, including automatic pre-barrier drains. Record-envelope negotiation
is independent of keyed-state ownership, preserving RowKinds for non-keyed buffers. Generated
two-phase SQL integration covers both backends with a test-only selection probe; full-type recovery,
in-flight channel replay and production memory admission remain unfinished.

The synchronous aggregate's transient allocation admissions distinguish input-derived copies,
historical output-event payloads, and materialized Arrow output. They are requested before their
respective allocations while loaded-state workspaces remain reserved. A budget denial before
commit discards staged updates; tests also verify scratch release. Historical string extrema use
one conservative per-batch allowance, avoiding per-row JNI admission calls. Mini-batch and bounded
allocation paths remain separate audit work; these checks do not remove their admission gates.

Accumulator workspace additionally reserves sparse counted-map nodes per distinct group and
per-row growth before state reads or decoding. The shared retained-size estimate includes a base
allowance even for an empty B-tree, since removing its last entry can retain a root allocation.
Same-thread test-only allocation observations cover sparse/dense maps and retraction shrinkage;
they do not instrument the release library or replace mixed JVM/native allocation profiling.

`NativeRegionStateParticipant` provides one Flink checkpoint participant for these shared owners.
Its raw keyed stream uses an `SFR1` envelope: a sorted list of node identities followed by each
node's opaque canonical snapshot in every key group. This framing keeps SQL state namespaces
separate during Flink key-group redistribution and is distinct from the legacy single-owner raw
stream. Changed node identities, duplicate key-group input, and incompatible headers are rejected;
the Flink initialization must fail and discard the region on any restore error.

Physical RocksDB checkpoints put each owner's files in a `node-<id>` directory. The existing Flink
file-checkpoint adapter uploads those directories in one handle and retains each namespace in
its SST reuse keys when incremental checkpoints are enabled. Shared-context checkpoint import uses the canonical key-group contract for the assigned
range and charges its temporary RocksDB reader and snapshot buffers to the task's memory budget.
There is no operator-specific checkpoint-import JNI bridge on this path.
Missing RocksDB `CURRENT` files are rejected rather than opening a new empty database during restore.

Generated two-owner deduplication tests compare post-restore changelogs with Flink, verify both
cross-backend canonical directions and split key-group assignments, and pass physical checkpoints
through the real file-checkpoint adapter with aligned and unaligned checkpoint options. Incremental
checkpoints reuse the same unchanged SST handles for both namespaces; full checkpoints upload
fresh private files. Restore also works after changing the incremental-checkpoint setting. These tests cover checkpoint
transport, not in-flight channel replay, timer recovery, or a planner-selected stateful topology.

The common multiple-input Flink runtime now accepts state-node identities from its factory and
uses `NativeRegionStateLifecycle` to bind all owners before `open`. The lifecycle derives the
key-group range and backend from Flink, shares one task memory owner/native context, registers the
region checkpoint participant, and restores before accepting records. RocksDB owners share the
assigned cache/write-buffer lease; embedded runners without a separate state-backend lease reserve
one fallback allowance from existing operator managed memory, not one allowance per state node.
Database handles close before that fallback lease is returned, including initialization failures.
Stable checkpoint staging lives outside the live database directory: Flink's asynchronous upload
retains its files after native state-owner close and owns their cleanup after materialization.
Closing the Flink keyed backend itself cancels its pending native uploads.

Runtime harness tests drive two timer-free deduplication stages through this actual common operator,
check Flink changelog bytes and record timestamps before and after restore, and exercise canonical,
aligned, and unaligned checkpoint options on both backends. The planner collector now supplies
regular-join and deduplication state identities through the shared fragment contract. One Flink
keyed multi-input transformation retains the planned external exchanges and their decoding contracts;
it rejects missing exchanges or incompatible routing domains. Additional runtime tests consume
hash-exchange frames and restore two owners across canonical memory/RocksDB savepoints. These tests do
not establish timer-driven semantics, channel-state replay, or complete state-specific metric parity.
The incremental uploader now uses an explicitly owned asynchronous snapshot task. Pre-start
cancellation removes staged files; cancellation during upload closes registered I/O and lets the
worker release its resources. Failed or cancelled attempts discard newly created remote handles,
never SST handles reused from an earlier checkpoint. Publication is coordinated with cancellation:
an unpublished result cannot report success or install a new SST-reuse map. Checkpoint storage's
`couldReuseStateHandle` decision is honored before reusing an SST. Focused tests cover cancellation
with and without interruption, blocked writes, partial-upload failure, publication races, and
backend closure in addition to the real RocksDB restore tests above.

State bindings require native plan protocol v2. At the shared input edge, RowKind and timestamp
metadata are attached once for the whole region, including retraction inputs and empty ports.
Only the metadata vectors are allocated; user column buffers are shared and retained until the
output stream closes. User payload field names beginning with `__streamfusion_` are rejected at
this boundary because that prefix is reserved for native transport metadata.

The shared Arrow output edge also recognizes the v2 native RowKind/ordinal envelope. Native
changelog kinds take precedence over input kinds when propagating an input's timestamps; user
column buffers remain shared. Input-ordinal timestamp propagation is only valid for operators
whose semantics select an input envelope, not arbitrary timer-created or historical output.

These shared APIs establish lifecycle integration; production admission is specific to the verified
in-memory binary equi-join subset. The generic Flink
region still needs migration of other physical families, timer/control dispatch for timer-driven operators, per-stage
state-specific metric integration, and full allocation admission. Other state operators remain
behind the whole-plan restriction above; the new bindings do not establish complete aligned/unaligned
Flink recovery parity for a multi-state runtime region or Nexmark performance parity.

## Backend contract

Native operators use a small backend-neutral Rust interface over opaque key and value bytes:
batched get, batched mutation, bounded ordered range visitation, canonical key-group snapshot,
and canonical key-group restore.
The in-memory backend can return borrowed values. The RocksDB backend implements a batch get with
one `multi_get` and a batch mutation with one `WriteBatch`.

Keys are prefixed or partitioned by the key group computed with StreamFusion's Flink-compatible
Rust key-group logic. This makes key-group ownership independent of the backend and lets Flink's
normal redistribution assign intersections during rescaling.

State component ABI version 8 transports read results and owned mutation keys/values as Arrow
`BinaryView` arrays. Large payloads retain producer-owned buffers across the C Data boundary;
the runtime does not concatenate mutations or copy every returned value into a second byte
vector. Inline values use Arrow's standard short-value representation. Runtime and plugin ABI
versions must match; incompatible components are rejected before use.

The ABI supports bounded key-group and ordered range scans. A request carries an inclusive start,
exclusive end, exclusive continuation key, row limit, and admitted byte limit; replies contain
standard Arrow `BinaryView` key/value columns. RocksDB seeks directly to the lower bound with its
bytewise comparator and stops at the upper bound. Callers may stop after any page. An entry larger
than the admitted page budget produces a recoverable resource error rather than exceeding the
budget. Scanning holds the backend stable until the operation completes.

A scan reply may carry optional Arrow schema metadata `streamfusion.state.scan.complete.v1`.
`true` says the requested range is exhausted, including when the final page reaches the row limit;
`false` says pagination must continue. This avoids another component call and RocksDB iterator
just to discover an empty final page. Absence retains legacy pagination until an empty result, so
ABI-8 components without the extension remain compatible. The function table, BinaryView column
schema and checkpoint encodings are unchanged. The C Data bridge preserves this schema metadata;
invalid completion values fail explicitly. Distinct partition ranges still have separate scans.

Range-oriented operators use a separately budgeted B-tree in-memory backend. Point-only operators
retain the existing hash-table backend. Both ordered backends read and write the same canonical
SFS1 snapshots, so key-group redistribution and backend changes preserve keys byte-for-byte.
Ordered entries include the existing Flink key-group identity, a length-framed operator partition
prefix, encoded ordering columns, and a deterministic sequence/row identity. Partition hashing is
unchanged. Payloads and small partition metadata are separate state entries.

The ordered operator format identifies StreamFusion encoding version 1 and Arrow row encoding
major version 59. Runtime and plugin ABI versions must match. Unknown operator encoding versions
are rejected. Existing operator state formats are read for migration; a migration and its triggering
input changes are committed in the same state batch. This does not establish savepoint
compatibility with arbitrary future Arrow versions. No deployment setting or separate memory
budget is introduced.

The optional RocksDB module is not linked into the central runtime. The runtime loads its versioned
C function table, exchanges batch requests through Arrow C Data, and calls RocksDB directly from
Rust. The component borrows Arrow-owned keys and values while constructing each native batch rather
than copying them into an intermediate object graph. No state lookup crosses JNI or asks Java to
understand the bytes. RocksDB's task-local WAL is disabled, matching Flink's keyed-state backend:
completed checkpoints plus replayable input, rather than the local database log, define recovery.

The native RocksDB component exposes RocksDB's physical checkpoint API. A transparent StreamFusion
keyed-state-backend adapter delegates ordinary Flink state to the configured HashMap or RocksDB
backend and claims only StreamFusion's marked native handles. Regular native RocksDB checkpoints
flush the checkpoint boundary to immutable SSTs and emit Flink's standard
`IncrementalRemoteKeyedStateHandle`. SSTs use shared scope; manifests, logs, and the small
StreamFusion metadata marker use exclusive scope. Empty RocksDB files are recorded in that marker
and recreated without inventing null state handles.

The planner installs this adapter in the `TableConfig` configuration before translation. This is
the configuration from which Flink creates the real pipeline; installing it only on the planner's
dummy `StreamExecutionEnvironment` would leave SQL jobs on the unwrapped backend. A planner-boundary
test observes the configured backend at translation time for both delegated HashMap and RocksDB
state. The shaded runtime resolves Flink's backend loader through its stable configuration and
class-loader parameters so SLF4J relocation cannot change the external Flink method signature.

For a planner-owned native operator, the adapter creates only a lightweight heap keyed-backend
shell for Flink's current-key and key-group lifecycle. Creating an otherwise empty Java RocksDB
instance beside the native database would duplicate cache reservations and checkpoint work. The
native database instead leases the task's normal Flink `STATE_BACKEND` managed-memory fraction;
Arrow buffers and operator scratch continue to use the operator's `OPERATOR` fraction. Operators
that do not carry StreamFusion's planner-owned identifier still receive the configured Flink
backend unchanged, so an all-or-nothing fallback remains an ordinary Flink RocksDB job.
As in Flink's embedded backend, all native RocksDB instances in the task manager share one LRU
block cache and one cache-charged write-buffer manager. The corresponding Flink shared-memory
resource is reserved once and reference-counted across operators, rather than multiplying the
state-backend fraction for every native database.

Both full and incremental regular RocksDB checkpoints use native checkpoint files. The existing
Flink `execution.checkpointing.incremental` setting controls reuse: when enabled, completed immutable
SST handles can be shared; when disabled, every file is uploaded in `EXCLUSIVE` scope and the
handle has no shared files. This follows Flink 2.3's `RocksNativeFullSnapshotStrategy`, which also
uses an `IncrementalRemoteKeyedStateHandle` to carry full private-file snapshots. The handle's
class name does not imply incremental reuse. No new configuration or upstream patch is required.

Previously, disabling incremental checkpoints routed native RocksDB state through a whole-key-group
canonical buffer. Large Q15 state exposed that mismatch with Flink. Regular checkpoints now avoid
that buffer while retaining Flink's synchronous consistency boundary, asynchronous upload,
cancellation and key-group restore lifecycle. Canonical savepoints still use the portable raw-keyed
format, and memory checkpoints retain their canonical path. Whole-group canonical savepoint
buffering remains a capacity limitation; this change does not solve it or retained HashMap growth.
Legacy operator checkpoint diagnostics count full uploads in checkpoint bytes but do not label
them as incremental checkpoints or increment SST-reuse counters.

Generated DISTINCT tests cover full RocksDB aligned/unaligned checkpoint restore and 1-to-2-to-1
rescaling, comparing every per-key changelog and record timestamp with the original Flink SQL
handler, including filtered counts and signed duplicate membership. Common two-owner tests verify
private-file handles and namespace restore. Full-file cancellation is exercised before execution,
during blocked upload and during stream finalization, with staging/handle cleanup and exactly-once
failure reporting. Existing incremental and canonical recovery tests remain in the focused suite.

The RocksDB checkpoint API itself establishes the synchronous immutable-SST boundary for
WAL-disabled writes. StreamFusion does not issue a redundant explicit flush before that call. The
component test checkpoints writes that have not otherwise been flushed, opens the checkpoint as a
fresh database, verifies every value, and then checks unchanged-SST reuse in the following
checkpoint.

The adapter reuses an SST only after its checkpoint completes, carries Flink's backend UUID and
physical state-handle identities through metadata serialization, and drops pending reuse state on
abort or subsumption. Tests round-trip the handle through Flink's version-3 durable metadata
serializer, verify unchanged SST identity and reduced checkpointed bytes, restore the round-tripped
handle, and rescale native RocksDB state 1-to-2-to-1. Canonical savepoints deliberately remain SFS1
raw keyed state so they can move between the native memory and RocksDB implementations.

The focused recovery matrix is shared by native group aggregation—including the stateful
split-DISTINCT/retractable-extrema incremental stage—deduplication, SELECT DISTINCT,
non-window Top-N, window aggregation, Window Deduplicate, Window Top-N, Window Join, and regular
and interval streaming Join, bounded hash/adaptive/nested-loop Join, plus Temporal Sort, bounded
full Sort, bounded SortLimit, and bounded partitioned Rank. Window, interval, and
temporal-sort cases include pending event-time and processing-time timers; two-input joins also
preserve their independently advancing input-watermark frontiers. Interval Join keeps its live timer
index in native memory and materializes dirty timer
groups into the backend at the checkpoint/savepoint boundary, so recovery remains canonical without
rewriting its complete timer group on every input batch:

| Recovery path | Memory state | RocksDB state |
| --- | --- | --- |
| Aligned checkpoint, same backend | Tested | Tested, incremental |
| Unaligned checkpoint, same backend | Tested | Tested, incremental |
| Canonical savepoint to memory | Memory → memory | RocksDB → memory |
| Canonical savepoint to RocksDB | Memory → RocksDB | RocksDB → RocksDB |
| Rescaling | 1 → 2 → 1 | 1 → 2 → 1 |
| Incremental metadata serialization and unchanged-SST reuse | Not applicable | Tested |

The exceptions to the rescaling row are Temporal Sort and bounded full Sort: SQL total ordering
requires Flink's singleton distribution, so their transformations are fixed at
parallelism/max-parallelism one. Their canonical formats are still backend-neutral. Both are
covered by aligned, unaligned, same-backend, and cross-backend recovery tests; bounded Sort and
SortLimit also test incremental RocksDB SST reuse, while Temporal Sort additionally preserves
pending timers. Bounded partitioned Rank retains Flink's hash exchange, supports rescaling, and uses
the same canonical Top-N state format across memory and RocksDB.

The operator processes a batch synchronously before the mailbox can snapshot it. Flink therefore
owns in-flight channel data for unaligned checkpoints, while the native keyed snapshot contains the
same completed state boundary as an aligned checkpoint.

## Reference gut-check

Flink's RocksDB full and incremental snapshot strategies are the lifecycle model: create a consistent native
checkpoint synchronously at the barrier, upload it asynchronously, reuse confirmed SST handles by
filename only when incremental checkpointing is enabled, upload mutable metadata privately, register shared handles with the checkpoint
coordinator, and only advance the reusable base after checkpoint completion. StreamFusion follows
that lifecycle while retaining its backend-neutral SFS1 savepoint format.

RisingWave's append-only deduplicate executor provides a useful operator-level comparison. It
projects all keys for a chunk, populates a managed cache from its state table, updates visibility
in input order, commits on barriers, and clears stale cache entries after vnode reassignment.
StreamFusion likewise preserves within-batch order and partitions state for rescaling, but its
RocksDB path intentionally uses one batch `multi_get` rather than per-key existence futures.

Arroyo does not currently expose an equivalent dedicated SQL deduplicate executor. Its
`GlobalKeyedTable` is the closest state comparison: generic keyed values live in a Rust `HashMap`,
updates are sent to an epoch checkpointer, and binary key/value Arrow arrays are written as Parquet.
That validates the opaque-byte and batch-checkpoint direction, while StreamFusion differs by using
Flink-owned key groups and snapshot lifecycle plus direct RocksDB incremental files.

## Canonical snapshot validation

Canonical restore validates the complete SFS1 payload before allocating decoded entries.
The entry count must fit the supplied byte length, every key/value length must be in bounds,
and trailing bytes are rejected. This prevents a corrupt entry count from driving an oversized
allocation; valid snapshots retain their existing byte format and cross-backend compatibility.
