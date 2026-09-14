---
title: RocksDB configuration
description: Flink settings supported by native RocksDB, resolution precedence, and remaining fallback conditions.
---

Native RocksDB uses Flink's existing configuration surface. These settings are supported for
otherwise eligible native stateful plans. They do not admit additional SQL semantics or stateful
operator families. Unsupported settings retain whole-plan Flink fallback with the option name
in the diagnostic. An already installed StreamFusion backend wrapper remains usable by
the fallback plan: unsupported native settings are rejected only when a native state owner
opens, before it reserves memory or creates a database.

## Database and table settings

All keys in this table start with `state.backend.rocksdb.`. Sizes use Flink's `MemorySize`
parsing; durations use Flink's duration parsing and are converted to whole seconds as in
`RocksDBResourceContainer`.

| Option suffix | Default | Native behavior |
| --- | --- | --- |
| `thread.num` | `2` | Maximum background flush/compaction jobs |
| `files.open` | `-1` | Maximum open files; `-1` leaves the limit unlimited |
| `log.max-file-size` | `25 mb` | Log rotation size; zero disables size-based rotation |
| `log.file-num` | `4` | Number of log files retained |
| `log.dir` | unset | Explicit absolute directory; logs remain user-owned after close |
| `log.level` | `INFO_LEVEL` | Debug, info, warn, error, fatal or header logging |
| `compaction.style` | `LEVEL` | `LEVEL`, `UNIVERSAL`, or `NONE` (no background compaction) |
| `compaction.level.use-dynamic-size` | `false` | Dynamic sizing of compaction levels |
| `compaction.level.target-file-size-base` | `64 mb` | Base target SST file size |
| `compaction.level.max-size-level-base` | `256 mb` | Base level byte limit |
| `writebuffer.size` | `64 mb` | Column-family memtable size |
| `writebuffer.count` | `2` | Maximum write-buffer count |
| `writebuffer.number-to-merge` | `1` | Minimum write buffers merged on flush |
| `compaction.filter.periodic-compaction-time` | `30 days` | Periodic compaction interval; zero disables it |
| `block.blocksize` | `4 kb` | Data block size |
| `block.metadata-blocksize` | `4 kb` | Metadata block size for partitioned indexes/filters |
| `memory.partitioned-index-filters` | `false` | Enables partitioned indexes and filters under the shared memory policy |
| `block.cache-size` | `8 mb` | Validated; Flink's managed shared cache overrides this setting |
| `compression.per.level` | `SNAPPY_COMPRESSION` | Per-level list: no compression, Snappy, Zlib, BZip2, LZ4, LZ4HC, or ZSTD |
| `use-bloom-filter` | `false` | Installs RocksDB's Bloom-filter policy |
| `bloom-filter.bits-per-key` | `10.0` | Passes the configured bits per key to that policy |
| `bloom-filter.block-based-mode` | `false` | Accepted; obsolete and ignored by both underlying RocksDB versions |

Positive counts and sizes follow Flink's configuration validation. Zero log size and zero
periodic-compaction time remain valid. Negative compaction durations cannot be represented by
the native unsigned duration and cause fallback. Configuring periodic compaction does not
expand TTL support; each operator retains its own TTL admission gate.

`state.backend.rocksdb.predefined-options` supports all four Flink presets: `DEFAULT`,
`FLASH_SSD_OPTIMIZED`, `SPINNING_DISK_OPTIMIZED`, and `SPINNING_DISK_OPTIMIZED_HIGH_MEM`. The SSD preset selects four background jobs and unlimited open files.
The spinning-disk preset additionally enables dynamic level sizing. Its high-memory variant
also selects 128 KiB blocks, a 1 GiB base-level limit, four write buffers with three merged on
flush, a 256 MiB target file size, and Bloom filters. Its nominal 256 MiB cache is overridden by
the Flink managed-memory resource, exactly as in Flink. Explicit settings take
precedence, including an explicitly configured default such as `thread.num=2` or
`compaction.level.use-dynamic-size=false`. The resolved backend's public preset setting is
preserved when the StreamFusion backend wrapper is serialized.

The wrapper also resolves `setPredefinedOptions` and `setWriteBatchSize` again when each native
keyed backend is created. Setters called after wrapping, including before serialization, reach
the new database and a database created for restore. Explicit configuration still overrides
preset values, including explicit defaults; the write-batch setter takes precedence over the
configured write-batch size. The wrapper retains its own copy of explicit options, so later
mutation of the caller's configuration object does not change the configured backend. Setters
affect subsequently created databases, not databases already open.

Older serialized Java backend wrappers that lack the explicit-option provenance must be
recreated from the Flink configuration before native execution. Guessing which values came from
a preset could silently lose explicit overrides. Rejection occurs before native memory or
database resources are acquired. This concerns the serialized job backend, not the persisted
checkpoint or savepoint state format.

Compression lists retain their order, including mixed codecs and uncompressed levels. The optional
RocksDB component builds Snappy, Zlib, BZip2, LZ4 (including LZ4HC), and ZSTD through the
upstream binding's build features. Set their existing Flink names in
`state.backend.rocksdb.compression.per.level`: `NO_COMPRESSION`, `SNAPPY_COMPRESSION`,
`ZLIB_COMPRESSION`, `BZLIB2_COMPRESSION`, `LZ4_COMPRESSION`, `LZ4HC_COMPRESSION`, or
`ZSTD_COMPRESSION`. RocksDB applies the list using the configured dynamic-level semantics.
An explicit empty list keeps the base Snappy compression, as Flink does. XPRESS and the
`DISABLE_COMPRESSION_OPTION` sentinel remain precise whole-plan fallbacks.

Compression options retain the defaults used by Flink's RocksDB dependency: default codec
level, zlib window bits -14 and strategy 0, and one compression thread. No codec tuning
options are added. SST files record their compression codec, so checkpoint readers and
reopened databases can read earlier files even when the destination selects a different
codec for future writes. Compression-library versions and physical file bytes can differ;
logical state and emitted changelog bytes must match. The existing shared managed cache,
write-buffer manager, and native execution reservations retain their ownership.

Bloom-filter bit sizing follows the underlying RocksDB policy: values below 0.5 disable filter
bits, values from 0.5 to 1 round up to 1, and values above 100 clamp to 100. The obsolete mode
flag has no effect in either Flink's FRocksDB 8.10 or native RocksDB 11.8. Direct option tests
also cover non-finite values; Flink's job-configuration serialization may reject these before
execution, so they are not supported deployment recommendations.

With `memory.partitioned-index-filters=true`, native RocksDB selects two-level indexes,
partitioned filters, and pinned top-level metadata. If Bloom filtering is enabled, Flink
replaces the configured filter with a **10-bit full filter**, even when a different bit count
was configured. Native execution preserves this rule. With Bloom filtering disabled, the
index remains partitioned and no Bloom policy is installed. Index and filter blocks stay
charged to the same Flink-managed cache.

`compaction.style=NONE` disables background compaction using RocksDB's upstream options parser;
it does not disable flushing or checkpoint durability. `UNIVERSAL` uses RocksDB's upstream
universal compaction implementation. FIFO remains gated: it can discard live state by age/size,
and equivalent state-eviction behavior has not been verified across the two RocksDB versions.

## Bulk writes

`state.backend.rocksdb.write-batch-size` controls the byte threshold for native bulk writes.
The default is **2 MiB**. Native execution uses the same policy as Flink's
`RocksDBWriteBatchWrapper`: flush after adding an entry when either the serialized RocksDB
write batch reaches the byte threshold or its entry count reaches **500**. Zero disables only
the byte threshold. An entry larger than the threshold is written in its own batch; the
setting is a flush trigger, not a hard maximum record size or a separate memory budget.
The configured backend's public `setWriteBatchSize` override survives wrapper serialization.

This splits dirty-state writes at the end of an Arrow/operator batch and during restore.
It does not move storage access into the computation loop, retain dirty writes across Arrow
batches, or introduce additional JNI crossings. All key groups are validated before the
first chunk is written. A later storage failure stops the flush; completed chunks cannot be
rolled back, so the failed native invocation must be discarded and restored through Flink's
existing failure lifecycle. Empty flushes issue no RocksDB write, and WAL remains disabled.

Native write batches count their actual serialized bytes, including native key-group prefixes.
Flink state keys can have different physical lengths. Tests isolate the same physical key
encoding and compare flush counts and checksums covering serialized write-batch bytes against
traces from Flink's actual bulk writer. Existing operator/batch reservations remain in force.
Chunking limits additional write-buffer growth instead of constructing a copy of the entire
dirty flush; the serialized-byte threshold is not an exact allocator-capacity ceiling. A
single oversized entry still needs its normal memory reservation. Initial allocation hints
are bounded. The upstream safe Rust API consumes each submitted
write batch, so StreamFusion creates the next buffer after submission rather than extending
the dependency with a borrowed-write entry point.

## Log directories

`state.backend.rocksdb.log.dir` is resolved with the database options and propagated through
backend serialization, every native database open, and physical checkpoint reads using the
destination task's configuration. It must be an absolute path. Relative paths and explicitly
empty values fail Flink's configuration validation; native admission also rejects NUL bytes.

When this setting is absent, StreamFusion retains Flink's TaskManager `log.file` lookup and
path-length guard for automatic relocation. Automatically relocated logs are cleaned after
the native database closes. An explicit directory takes precedence over that lookup and
**its current and rotated logs survive backend cleanup**, as in Flink. RocksDB still applies
its configured log rotation and retention limits while running.

RocksDB creates a missing log directory when its parent exists. An unusable directory, missing
parent, or overlong flattened database-path filename fails database opening in both the actual
Flink FRocksDB 8.10 dependency and native RocksDB 11.8. Explicit directories do not silently
switch to database-local logs for these failures. This follows the inspected implementations
and database-open tests; the older Flink option description suggesting a local-log fallback
does not describe these versions' behavior. State and checkpoint file ownership remain with
Flink; this setting affects only RocksDB information logs.

## Local state directories

`state.backend.rocksdb.localdir` is supported with Flink's comma/platform-path-separator list
syntax. StreamFusion uses Flink's public path parser for validation and preserves configured
backend paths through serialization, including file URIs set through the backend's public
`setDbStoragePaths` API. The configuration string retains Flink's separator grammar; a URI
accepted by the programmatic API is not necessarily valid in that string.

At the first native keyed backend creation, the Java adapter probes each configured root by
creating and removing a unique temporary directory, matching Flink. Unusable roots are logged
and skipped; if none are usable, initialization fails before reserving native RocksDB memory.
Usable roots are selected in round-robin order from a random starting point for successive
keyed backends belonging to that backend instance. Selection is initialized again after the
backend is deserialized on a worker. Without this option, native RocksDB uses Flink's
TaskManager temporary working directory.

Filesystem roots are resolved before native path serialization, so symbolic links and parent
components retain their filesystem meaning instead of being redirected by lexical normalization.
Each native region creates a unique `streamfusion-region-state-...` directory under its
selected root. Its `node-<id>` children preserve state identities within the fused plan;
this native directory layout differs from Flink's per-operator database layout because one
native region owns several stateful stages. Physical checkpoint staging uses a separate
sibling directory on the same filesystem. Restore downloads use the destination keyed
backend's selected root and are removed after successful or failed import. Closing a region
removes its live directory; shared roots, neighboring files, and checkpoint files still owned
by asynchronous Flink uploads are preserved. Ordinary DataFusion spill assignments continue
to come from Flink's IOManager.

## Checkpoint file transfers

`state.backend.rocksdb.checkpoint.transfer.thread.num` now controls native checkpoint uploads
and restore downloads. StreamFusion reads the resolved public Flink backend setting after
configuration/serialization and uses the upstream `RocksDBStateDataTransferHelper`:

- The default is four transfer threads per native keyed backend (fused region).
- Configured values above one use a dedicated fixed pool of that size.
- Configured zero or one executes transfers directly on the snapshot/restore caller.
- Negative values other than `-1` borrow the TaskManager I/O executor with Flink's job context.
- In Flink 2.3, `-1` is also the backend's unset sentinel: its public getter resolves it to four
  threads. Native execution preserves this behavior. The programmatic setter retains Flink's
  stricter positive-only validation.

Flink owns the returned keyed backend's lifetime. The backend closes its dedicated transfer
helper on close/dispose and leaves a borrowed TaskManager executor running. The short-lived
registry supplied for backend construction owns cancellation while restore files are being
prepared. Successful construction detaches the transfer helper from that registry, so Flink's
normal end-of-construction close does not shut down the runtime executor.

Transfers run concurrently while result assembly retains the original sorted file order,
state-node namespaces, empty-file metadata, full-checkpoint private-file behavior, and
incremental SST reuse. Uploaded/checkpointed byte counts use the returned storage handles'
sizes, including storage encoding overhead, as Flink does. Local file sizes remain the SST
reuse eligibility check. No native operator input/output shape or memory budget changes.

Cancellation closes active I/O, cancels queued work, and waits for running transfer workers to
exit before removing staging or discarding unpublished handles. A handle returned late from
storage finalization is still discarded exactly once. Failed restore downloads remove their
own staging directory and preserve the checkpoint's remote handles. Metadata format and
physical checkpoint formats are unchanged; duplicate destination paths are rejected before
parallel restore starts.

### Restore preparation and cancellation

Native file-checkpoint materialization completes during keyed-backend creation, inside Flink's
`BackendRestorerProcedure`. File-path validation, empty-file metadata parsing, and download
failures therefore reject that candidate before Flink returns a backend. If another candidate
is available, Flink performs its normal retry with fresh resources. Registered fused native
regions support both remote handles and the local handles selected by Flink; see
[local recovery](#local-checkpoint-backup-and-recovery) below.

The preparation phase uses the destination backend's selected local roots and configured
transfer executor for remote files. Local files use the restoring task thread, as in Flink.
Only handles intersecting the assigned key groups are materialized. Metadata
identification reads, metadata decoding, active file streams, and queued transfer work participate
in Flink's construction cancellation. Metadata is decoded from its stream without first buffering
the complete file. A failed later handle removes all earlier prepared directories from that
candidate, and backend-creation failure releases the Java shell and native memory lease.
Remote checkpoint handles remain owned by Flink throughout.

For registered fused native regions, native database opening and semantic file-state import now
also complete before keyed-backend creation returns. Corrupt native files reject the candidate
within Flink's retry procedure. This uses the same native execution context, configuration,
STATE_BACKEND cache lease, OPERATOR reservations, and metric tree that the operator subsequently
adopts. Standalone legacy participants still import after registration; they do not establish
local-recovery support.

Flink creates its keyed backend before its operator-state backend. Window validation needs the
minimum restored union watermark even for empty key groups and legacy session migration. Native
preparation therefore uses Flink's own `DefaultOperatorStateBackendBuilder` to read only the
named window-clock partitions through `OperatorStreamStateHandle` views. Original checkpoint
handles remain unchanged and Flink-owned. Ordinary operator-state restoration later runs as
usual; native initialization checks that its clock values exactly match those used during
preparation, then attaches the real operator state. No Arrow payload or per-record data passes
through this preparation path. The additional read concerns small control-state clock records,
not native keyed state, and requires no private Flink patch or native ABI change.

Metrics register after native database creation and before import. A failed candidate closes
its metric registration, native context, reservations, and prepared files before retry. The
keyed backend owns the prepared context until operator initialization adopts it. After that
handoff, the operator owns the context and Arrow allocator, so backend close cannot invalidate
batches retained by its dispatcher. Raw/canonical savepoint restoration retains its existing
operator-initialization path; this preparation phase concerns physical file checkpoints.

`NativeCheckpointRestoreCreationTest` exercises actual wrapper creation with Flink's retry
procedure, failed later handles, metadata/file cancellation, pre-import close, and subsequent
checkpoint uploads after the construction registry closes. Tests cover direct, dedicated, and
borrowed transfer executors. Unchanged upstream backend-restorer tests and upstream aggregate SQL
coverage run alongside generated aggregate, window-clock, channel-recovery, and local-backup
parity tests. `RocksDbNativeRestoreBootstrapParityTest` additionally corrupts a native CURRENT
file and uses Flink's actual retry procedure on the same native operator, checking that failed
native creation releases memory and the successful candidate completes import before returning.
Generated session, processing-time, and shared-window tests compare restored clocks and complete
changelogs. Additional local-recovery tests below exercise Flink's local-first selection and
remote retry through the actual operator harness.

## Memory and recovery

The existing [write-buffer and high-priority cache ratios](/StreamFusion/development/native-state/#configurable-rocksdb-shared-memory)
remain configurable. Database/table options do not create another memory budget or change
shared-cache ownership. The first owner of a shared Flink memory resource determines its
partitioning policy along with its memory ratios; subsequent owners use those resolved settings.
Databases sharing one Flink STATE_BACKEND resource may use different
table and write-buffer settings while sharing the same cache and write-buffer manager. Shared
memory pressure can trigger flushing before a configured column-family write-buffer limit.

The backend factory resolves settings once, stores them in the serializable Flink backend, and
binds them to every native state owner in the fused plan. State-binding protocol 6 carries the
base database options; protocol 7 adds filters, compression and log level, and protocol 8 adds
the shared partitioning flag and compaction style. Protocol 9 adds explicit log-directory
ownership; protocol 10 adds the bulk-write threshold. State-component ABI 19 copies scalar settings and a borrowed compression-code span during database open; both
native libraries must use that ABI; ABI 16 introduced the expanded codec set, ABI 17 adds the bulk-write threshold, ABI 18 adds statistics readers, and ABI 19 adds their coarse memory-admission query. Compression-list decoding and native option copies use
a coarse setup reservation retained with the state bindings; unusually large lists must fit the
existing Flink execution budget. Older state bindings without database options retain
Flink's defaults. Protocol-6 database options retain disabled Bloom filters, Snappy compression
and info logging when the newer fields are absent. Bindings without the partitioning flag or
compaction style retain unpartitioned indexes and level compaction. Bindings without a write threshold
use Flink's 2 MiB default and the default bulk entry-count limit.

Native physical checkpoint readers use the destination task's settings, as do its live
databases. Restoring a checkpoint does not restore obsolete runtime tuning values. State
encodings and checkpoint file formats are unchanged. Flink retains checkpoint coordination,
file ownership, restore assignment, and the managed-memory lease.

## Backend creation and programmatic settings

The planner rejects unsupported settings before replacing the whole job. Native keyed-backend
creation also checks the supplied configuration, including optional metrics and checkpointing
during channel recovery. These errors are retained until a native owner requests the backend;
the wrapper remains usable by the ordinary Flink fallback.

Public programmatic overrides for fixed-per-slot memory, unmanaged memory, custom
`RocksDBOptionsFactory`, and heap timer services are checked again when the native backend is
created, including overrides made after wrapper construction and preserved by serialization.
An unsupported override fails before selecting a storage root, reserving state memory, or
starting checkpoint-transfer workers. Native validation does not invoke the custom factory's
DB/column option callbacks. Silently replacing a fixed budget with a managed fraction, or
ignoring a factory or timer-service choice, is not supported behavior.

The standard StreamFusion backend factory supplies the configuration used to construct its
Flink delegate. Code constructing `StreamFusionStateBackend` directly must pass that same
`ReadableConfig` when the delegate was explicitly configured. The one-argument constructor
is for a default-configured delegate; public overrides already supported by StreamFusion,
such as preset, storage directories, transfer thread count, write-batch size, and managed
cache geometry, retain their existing propagation paths.

Flink's `RocksDBSharedResourcesFactory` selects fixed-per-slot memory before managed memory,
then uses the cluster's fixed-per-TaskManager setting when managed memory is disabled. Fixed
allocations use external shared resources rather than debiting the slot's managed pool.
A native cache allocated separately from Java source/sink state could therefore exceed the
single fixed total promised by Flink. StreamFusion keeps fixed budgets gated until that
sharing contract can be preserved across both RocksDB libraries; it does not reinterpret
these settings as native managed-memory fractions. The TaskManager case also requires
cluster-level configuration and sharing across slots.

## Evidence and remaining fallback

Source comparisons use Flink 2.3's `RocksDBResourceContainer`, `RocksDBConfigurableOptions`,
`PredefinedOptions`, and `EmbeddedRocksDBStateBackend`. Generated Java tests compare resolved
settings with the actual Flink resource container, including preset override precedence and
zero-value semantics. Rust tests inspect the opened database's persisted OPTIONS file and
exercise shared cache ownership and read-only checkpoint access with different destination
settings. Filter comparisons also inspect the FRocksDB 8.10 JNI and filter-policy implementations
against native RocksDB 11.8. Compression tests compare enum codes and mixed-list mapping with
the actual Flink resource container, require actual SST compression for every compiled codec,
and read checkpoints and reopen live databases under different destination codecs. Core
state tests restore ZSTD checkpoints into an LZ4-configured destination. Tests inspect filter sizes, verify
rounding and mode-flag behavior, and compare the actual Flink shared-cache capacity under the
high-memory preset and explicit cache-size overrides. Partitioning tests compare the actual
Flink table configuration, the ten-bit override, native persisted options and SST filter sizes,
shared-owner conflicts, and checkpoint reads under changed destination settings. Log-directory
tests open actual Flink and native databases with generated paths, verify retained logs and
matching failure cases, and restore checkpoints with different destination log directories.
Local-directory tests compare actual Flink root selection, rejected paths, skipped unusable
roots, round-robin selection, and programmatic configuration after serialization. Generated SQL
runs inspect retained native log filenames to verify actual database placement and check that
live directories disappear without removing user files. Physical checkpoint tests verify
restore download placement and cleanup while comparing state and rescaled key-group contents.
Bulk-write tests compare generated traces against Flink, validate oversized values and
zero/count-only behavior, reject invalid trailing key groups without partial writes, and
round-trip chunked puts/deletes through physical checkpoints. Transfer tests compare executor selection and ownership with the actual Flink helper, bound
observed concurrency, and exercise queued-work rejection, late upload finalization, download
cancellation, deterministic handle order, storage-handle size accounting, and SST reuse.

Backend-creation tests exercise public overrides before and after wrapping and serialization,
verify that rejected native opens leave memory and storage untouched, and write/read generated
state through the same wrapper's actual Flink fallback. Configured SQL fallback tests compare
complete changelogs and require zero native batches for the rejected settings.

SQL tests compare complete generated Flink/native aggregate changelogs and execute the published
Flink binary-string aggregate test with acceleration required. Operator harnesses compare the
complete registered metric surface and deterministic values. Recovery tests compare aligned
and unaligned channel replay with Flink on both backends. These checks establish configuration
and correctness behavior; no performance improvement is claimed.

XPRESS and the disable-compression sentinel, FIFO compaction, the
`NUM_INFO_LOG_LEVELS` log-level sentinel, custom options factories, fixed or unmanaged memory budgets,
unimplemented restore/compaction settings still cause
whole-plan fallback. Column-family RocksDB properties, keyed-state latency histograms, and
checkpointing during channel recovery retain their separate gates. No StreamFusion tuning options are introduced.

## Complete backend option inventory

The Flink 2.3.0 `RocksDBOptions`, `RocksDBConfigurableOptions`, and
`RocksDBManualCompactionOptions` classes expose **46 options**. Twenty-eight follow the
implemented configuration/resource paths described above. Seven manual-compaction settings follow
Flink's disabled-manager branch, described below. The remaining eleven are accepted only at their
audited defaults and cause whole-plan fallback when changed:

| Option suffix under `state.backend.rocksdb.` | Required default | Missing native behavior |
| --- | --- | --- |
| `memory.managed` | `true` | Unmanaged native cache/write-buffer ownership |
| `memory.fixed-per-slot` | unset | One fixed cache shared with Java owners |
| `memory.fixed-per-tm` | unset | One TaskManager cache shared with Java owners |
| `options-factory` | unset | Translation of arbitrary factory callbacks |
| `timer-service.factory` | `ROCKSDB` | Verified alternate timer-backend behavior |
| `timer-service.cache-size` | `128` | Flink's per-key-group RocksDB timer cache |
| `restore-overlap-fraction-threshold` | `0.0` | Selection of an initial database by handle overlap |
| `use-ingest-db-restore-mode` | `false` | Exported-column-family import during restore |
| `incremental-restore-async-compact-after-rescale` | `false` | Post-rescale range compaction scheduling |
| `rescaling.use-delete-files-in-range` | `false` | File deletion during key-group clipping |
| `compaction.filter.query-time-after-num-entries` | `1000` | TTL compaction-filter timestamp refresh |


The default timer-cache value does not mean native timers implement Flink's heap front cache;
current native timer storage remains operator-specific. Default restore uses validated, admitted
key-group import into the destination database. Default TTL behavior remains subject to each
operator's TTL support. Manual compaction is disabled at the default interval.

Fallback messages identify the option and the specific missing behavior. An upstream backend
option absent from the audited inventory causes fallback even at its default, since a new
default can change behavior. `NativeRocksDbOptionInventoryTest` compares the complete reflected
Flink option set with the implementation, separates conditional settings, and pins the eleven
default-only values. Metric
options, common checkpoint options, and unsupported enum values within otherwise supported
options retain their separately documented checks.

Mutable-setting tests compare actual Flink resource-container options with bindings emitted by
native keyed-backend creation, before and after wrapper serialization. Generated aggregate tests
inspect the native database's persisted options and compare complete Flink changelogs and
timestamps across aligned and unaligned physical restore with changed presets and batch sizes.

## Disabled manual-compaction settings

The six settings below no longer cause fallback while Flink's manual-compaction manager is
disabled. They retain Flink's own parsing and dormant behavior; this does not implement native
manual compaction or change RocksDB's automatic compaction settings.

| Option suffix under `state.backend.rocksdb.manual-compaction.` | Flink default |
| --- | --- |
| `max-parallel-compactions` | `5` |
| `max-file-size-to-compact` | `50 kb` |
| `min-files-to-compact` | `5` |
| `max-files-to-compact` | `30` |
| `max-output-file-size` | `64 mb` |
| `max-auto-compactions` | `30` |

Flink 2.3.0 `RocksDBManualCompactionConfig.from` parses all seven settings and converts
`min-interval` to whole milliseconds. `RocksDBManualCompactionManager.create` returns its no-op
manager when that resolved interval is nonpositive. StreamFusion uses the same parser and
condition. The default interval is zero; a positive interval below one millisecond also resolves
to zero in Flink. Invalid values still fail configuration parsing even when compaction is
disabled. An interval of one millisecond or more retains precise whole-plan fallback naming
`state.backend.rocksdb.manual-compaction.min-interval`: native small-SST selection and the
manual-compaction scheduler remain unimplemented.

No compaction scheduler, background executor, native ABI field, or extra memory allocation is
introduced for dormant settings. Existing wrapper serialization and creation guards preserve
admission and reject enabled scheduling before native resources are acquired. As with other
settings, direct wrapper construction must receive the same configuration used for its Flink
delegate. Tests invoke Flink's actual no-op manager, check all malformed fields, compare generated
SQL changelogs on both backends, run the upstream aggregate SQL test with all six settings changed,
and exercise real local/remote checkpoint recovery through the TaskManager lifecycle.

## Local checkpoint backup and recovery

`execution.checkpointing.local-backup.enabled=true` now retains a local copy of native
RocksDB file checkpoints. It supports both full and incremental checkpoints and does not
change aligned or unaligned checkpoint handling. The runtime reads the task's resolved Flink
`LocalRecoveryConfig`; Flink selects and rotates the roots configured by
`execution.checkpointing.local-backup.dirs` (including its deprecated alias). No StreamFusion
backup directory setting or separate memory budget is introduced.

Each native backend creates its checkpoint under its own namespace within Flink's subtask
checkpoint directory. RocksDB creates the stable checkpoint files; adjacent native stages retain
their existing `node-ID` directories. Metadata uses Flink's duplicating checkpoint stream and
lives outside those node directories. The result contains Flink's remote handle and an
`IncrementalLocalKeyedStateHandle`, including when remote files are all private for a full
checkpoint. Checkpoint/savepoint state encodings are unchanged. Canonical savepoints keep their
existing raw-state path; this feature concerns native file checkpoints.

Until the snapshot future publishes its result, StreamFusion owns new remote handles, local
metadata, and checkpoint files. Cancellation or upload failure removes them without discarding
reused remote SST handles or another chained operator's backup. After publication, Flink's local
state store owns backup retention, pruning, and discard; closing the native backend does not
delete that backup. Local metadata failures preserve Flink's remote-only result behavior and
remove the unusable local directory. Failure to create the database checkpoint still fails the
snapshot, as in Flink. Remote upload/reuse metrics keep measuring remote physical bytes; local
handle size measures the actual local files and metadata using Flink's definitions. Logical
record and operator metric semantics are unchanged.

`execution.state-recovery.from-local=true`, including the deprecated
`state.backend.local-recovery` alias, enables native RocksDB local recovery for registered fused
native regions. Flink supplies the prioritized local/remote handles and decides when a failed
local candidate should retry from remote state. StreamFusion materializes and opens native state
inside Flink's `BackendRestorerProcedure`, before backend creation returns. Missing directories,
missing or truncated metadata, and corrupt manifests or SSTs fail that candidate; failed native
contexts, metrics, memory reservations, and private staging directories are released before
Flink tries the next candidate. Healthy local restoration does not download remote data files.

Restoration creates private staging under the destination's configured RocksDB local root. It
hard-links immutable SST files, copies mutable files, and falls back to copying an SST when the
filesystems do not support the link. Cancellation closes active streams and removes staging;
restoration never consumes or deletes Flink's retained local backup. Native import uses the
destination configuration and assigned key-group range, preserving the existing file-state
encoding and shared-memory ownership. Local metadata is validated before import, including empty
files and path ownership. Flink's published checkpoint handles and subsequent local-store discard
remain authoritative. This does not change Arrow operator boundaries or the native ABI.

The task's resolved `LocalRecoveryConfig` is authoritative, including backup-directory selection
and Flink's default of enabling backup when recovery is enabled and backup is unspecified. An
unregistered standalone/custom native owner cannot guarantee native opening inside Flink's retry
boundary: it rejects local recovery before reserving native resources, with an explicit reason.
This restriction does not apply to the registered fused runtime used by planner-admitted native
regions. Canonical savepoints retain their existing raw-state path.

Source comparison uses Flink's `RocksDBSnapshotStrategyBase`, `RocksIncrementalSnapshotStrategy`,
`CheckpointStreamWithResultProvider`, `IncrementalLocalKeyedStateHandle`, `TaskLocalStateStoreImpl`,
`RocksDBHandle`, `RocksDBIncrementalRestoreOperation`, `PrioritizedOperatorSubtaskState`, and
`BackendRestorerProcedure` at `release-2.3.0`. Tests run unchanged upstream metadata-stream and
backend-retry contracts and the applicable upstream aggregate SQL body with recovery enabled.
Generated SQL tests cover both modern and deprecated recovery settings and assert ordinary
whole-plan acceleration with exact changelogs.

`RocksDbLocalRecoveryParityTest` exercises actual Flink local-first selection across
full/incremental and aligned/unaligned checkpoints. It compares complete aggregate changelog
bytes and timestamps, forbids remote file reads for a healthy backup, damages five different
backup components to force remote retry, checks destination `max_background_jobs` in persisted
RocksDB options, checks the complete Flink operator metric surface after retry, and takes a
subsequent checkpoint. `RocksDbLocalWindowRecoveryParityTest`
compares shared and attached windows across aligned/unaligned local restoration and remote
retry, including restored clocks before replay, generated late/future inputs, output/control
bytes, and late-record metrics. These harness tests establish recovery behavior, not a measured
recovery speedup or a TaskManager-loss benchmark. Existing remote/canonical tests retain
cross-backend and rescaling coverage; this local-first matrix restores to the same parallelism.

`RocksDbTaskLocalRecoveryParityTest` additionally runs ordinary SQL on a MiniCluster with real
tasks and TaskManager local stores. For full/incremental and aligned/unaligned checkpoints, a
checkpointed source fails after a completed checkpoint containing nonempty aggregate SSTs; the
source offset, aggregate state, and complete collecting-sink changelog restore together. The
matrix compares Flink against native local recovery and native local-to-remote retry after a
corrupt CURRENT file. A test-only observing backend verifies the actual handles selected by
Flink and native import completion before backend creation returns. It does not change planner
selection or operator algorithms. Backup is left unspecified to exercise Flink's default when
local recovery is enabled. The unchanged upstream task-local-store manager tests additionally
cover option resolution, allocation-directory retention, and cleanup.

The task test exposed and fixed an unaligned exchange ordering issue: native frames now preserve
contiguous key-group runs instead of collecting all rows of each group. This preserves complete
per-channel changelog order during normal processing and recovery; see
[exchange ordering and memory](/StreamFusion/operators/exchange/#implementation). The tests use a
deterministic persisted-state checkpoint and a task failure within a running TaskManager. They do
not establish TaskManager-process replacement, local-first rescaling, or a recovery speedup.

Ownership tests also exercise actual Flink local-store retention and abort, backup-only restore
selection, local metadata failure, chained namespaces, cross-filesystem SST copying, and
cancellation before/during upload, during restore metadata reads, and after metadata completion
but before publication. Generated backup tests compare every nonempty local checkpoint file
against its remote bytes.

## Checkpointing during channel recovery

`execution.checkpointing.during-recovery.enabled=true` retains whole-plan Flink fallback on
both state backends. Ordinary aligned and unaligned restore remain supported. The diagnostic
names the option and a reproduced Flink 2.3 local-channel recapture condition; this is no longer
only a missing-verification gate.

In the published Flink **2.3.0** implementation, `RecoveredInputChannel.toInputChannel` can move
filtered but unread buffers into a live `LocalInputChannel` before replay finishes. A first
unaligned barrier on another input starts checkpointing on this channel too. Its
`checkpointStarted` captures the queued buffers, and consuming one while its own barrier is
still pending calls `ChannelStatePersister.maybePersist` on that same buffer again. The
`FlinkRecoveredChannelCheckpointContractTest` reproduces these two writer submissions using
unmodified Flink classes and its recording writer; a completed-checkpoint control submits once.
`ChannelStateWriterImpl.addInputData` forwards these submissions without sequence-number
deduplication. This is a channel-checkpoint boundary, independent of RocksDB configuration.
Retaining Flink fallback does not repair the underlying Flink behavior.

The recovery filter itself has separate generated compatibility coverage.
`GeneratedExchangeRecoveryParityTest` sends actual native Arrow IPC frames and equivalent
Flink rows through Flink's `ChannelStateFilteringHandler` on independently fragmented input
gates. It checks every destination at parallelism 1, 3, and 7, exact surviving frame bytes,
per-key changelog bytes, timestamps, watermarks, watermark status, and buffer release. A large
variable-width frame verifies actual temporary-file spilling and cleanup. Nine unchanged
upstream record-filter/factory tests also run from Flink's test JAR.

These tests establish the filter boundary, not full checkpoint-during-recovery support. Removing
the gate requires an upstream lifecycle fix plus complete overlapping checkpoint, replay,
failure, and rescaling validation with native state. StreamFusion does not patch Flink's channel
or checkpoint algorithms to bypass this requirement. The source audit uses the `release-2.3.0`
tag matching the dependency, rather than Flink's development branch.

## RocksDB ticker metrics

The eleven existing Flink ticker options below are supported on native RocksDB state. They are
all disabled by default. Configuration is resolved through Flink's own native-metric options,
serialized with the state backend, and bound to every native stateful stage using protocol 11.
On the in-memory backend, these RocksDB-only settings produce no metrics, matching Flink.

Each option is under `state.backend.rocksdb.metrics.`. All metrics are `Gauge<Long>` values in
the original physical operator's metric group, with no column-family subgroup.

| Option suffix | Gauge name | Unit |
| --- | --- | --- |
| `block-cache-hit` | `rocksdb.block_cache_hit` | Hits |
| `block-cache-miss` | `rocksdb.block_cache_miss` | Misses |
| `bloom-filter-useful` | `rocksdb.bloom_filter_useful` | Useful negative filter results |
| `bloom-filter-full-positive` | `rocksdb.bloom_filter_full_positive` | Positive full-filter results |
| `bloom-filter-full-true-positive` | `rocksdb.bloom_filter_full_true_positive` | True positive full-filter results |
| `bytes-read` | `rocksdb.bytes_read` | Bytes returned by Get |
| `iter-bytes-read` | `rocksdb.iter_bytes_read` | Iterator key/value bytes |
| `bytes-written` | `rocksdb.bytes_written` | Bytes written by RocksDB writes |
| `compaction-read-bytes` | `rocksdb.compact_read_bytes` | Compaction input bytes |
| `compaction-write-bytes` | `rocksdb.compact_write_bytes` | Compaction output bytes |
| `stall-micros` | `rocksdb.stall_micros` | Microseconds of write stalls |

Upstream ticker definitions remain authoritative. Both Flink's FRocksDB and the native component
count MultiGet bytes separately from `BYTES_READ`; native batched state access therefore does not
fabricate Get byte counts. Physical counts measure actual accelerated storage work and can differ
from Flink's counts. Logical record counters remain unchanged. Values preserve upstream unsigned
64-bit bits in Java's signed Long representation, as Flink's ticker monitor does.

Ticker views register after database creation and before checkpoint restore, matching Flink's
RocksDBHandle lifecycle. They start at zero and cache their values until Flink's view updater runs. All ticker
views in a fused region share one native sample per updater round. Sampling uses a separate JNI
channel and independent statistics readers, without taking an operator execution lock or adding
per-record JNI work. The metric tree closes its sampler before the execution context, waits for
an active sample to finish, releases the callback, and freezes the last values. An exceptional
sampling failure also freezes this cohort and logs once: the JNI snapshot can fail memory
admission, while Flink's shared ViewUpdater does not isolate failing views. This prevents the
failure from stopping unrelated TaskManager metrics.

A statistics reader owns the upstream statistics reference independently of the database. Its
options are captured before attaching caches, write-buffer managers, table resources, or log
paths. Temporary physical checkpoint readers share the destination stage's statistics and memory
reservation, while keeping their own cache geometry and log ownership. Restore reads contribute
to the destination stage's counters; source-run counters are not restored from the checkpoint.
Unknown, duplicate or excessive ticker codes fail before any database is opened.

### Statistics memory

Enabling upstream statistics allocates per-CPU ticker and histogram arrays even when only ticker
metrics are selected. State-component ABI 19 reports a coarse requirement computed from the
bundled unmodified RocksDB types and the same CPU/shard-count rule as `CoreLocalArray`. A small
StreamFusion-owned C++ translation unit computes this admission metadata; it does not modify
RocksDB, its bindings, statistics implementation, or storage behavior.

The runtime reserves this amount before enabling statistics, from the existing shared Flink
native-execution budget. On the 16-CPU development host the array bound is 1,290,240 bytes;
64 KiB of additional coarse headroom covers object, option and allocator metadata. CPU count and
platform type sizes determine the requirement on other hosts. This internal estimate is not a
deployment knob or separate memory budget. The STATE_BACKEND cache/write-buffer reservation is
unchanged. A database and its detached/temporary readers share one statistics reservation, which
is released only after the last owner closes. Disabled statistics take no statistics reservation.
Admission denial and failed database opens release their credit without leaving an opened DB.

Both native libraries must negotiate ABI 19. Older state bindings keep statistics disabled;
persisted checkpoint encodings remain unchanged. Generated native and JNI tests cover admission,
last-reader ownership, unchanged state bytes, concurrent sampling, and shared physical restore.
Java tests check every upstream ticker mapping and backend serialization. SQL tests compare full
changelogs on both backends, run the applicable upstream Flink SQL body with all tickers enabled,
and cover aligned/unaligned recovery. Selected-region metric tests compare the full Flink surface,
deterministic logical values, actual physical-counter definitions, cached updates and close.
Column-family property metrics and keyed-state latency histograms remain precise whole-plan fallbacks.
