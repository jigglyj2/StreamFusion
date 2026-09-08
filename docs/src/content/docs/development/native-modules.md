---
title: Native modules and ABI
description: Packaging optional Rust implementations and connecting them without batch copies.
---

StreamFusion keeps optional integrations optional all the way down. The Rust code for
an operator or connector belongs to the Maven module that implements that integration,
along with its platform-specific shared libraries. A Kafka connector, for example,
must not add its client or Rust implementation to a mandatory, central native binary.

The owning module is responsible for building, packaging, resolving, and loading its
native artifacts. A platform JAR may carry the appropriate `.so`, `.dylib`, or `.dll`
as a resource and extract it to a safe, versioned location before loading it, similar
to ADBC's JNI library packaging. Unsupported operating-system or architecture
combinations must be detected before planning an accelerated job and produce a clear
fallback reason.

## Release CPU and platform matrix

Routine pull-request CI builds only the native library needed by its test runner. The
release-only workflow builds optimized Rust libraries on separate GitHub-hosted Linux
and macOS runners and assembles them into a platform resource JAR:

| Platform | Packaged CPU variants |
| --- | --- |
| Linux x86-64 | x86-64-v2 baseline, x86-64-v3 (AVX2), x86-64-v4 (AVX-512) |
| Linux ARM64 | Native feature set of the ARM64 release runner |
| macOS x86-64 | Native feature set of the Intel macOS release runner |
| macOS ARM64 | Native feature set of the Apple Silicon release runner |

The loader verifies each library's own CPU metadata and SHA-256 before loading it. It tries
an eligible native build, followed by eligible v4, v3 and v2 artifacts on Linux x86-64.
Portable variants require the complete declared feature set, including inherited baseline
features. Native builds additionally require the recorded CPU identity and all build-host
features on every worker processor. This is deliberately conservative: a different CPU model
may need a portable variant or a new native build even if some instruction sets overlap.
Linux ARM64 and macOS native resources have the same identity/feature checks. Missing metadata,
unknown metadata versions, mismatched checksums and incompatible CPUs are rejected.

The optional RocksDB Maven module packages its own Linux x86-64 library and CPU metadata;
it is not included in the core platform JAR. Its planner preflight reports a specific
whole-plan fallback reason for a missing or incompatible component. Workers repeat validation
before opening native state. A worker incompatible with the submitted plan fails initialization
and uses Flink's normal failure handling; Java cannot replan an already deployed task locally.
Use an artifact compatible with every assigned worker.

Maven and the release workflow build through `dev/native/artifact.py`. This wrapper always
uses release optimization, frame pointers and profiling symbols, and writes metadata only
after a successful build. CPU requirements also enter the Cargo fingerprint so a cache moved
between native build hosts cannot silently retain another machine's code. Direct Cargo builds
remain useful for Rust unit tests, but JVM execution requires the matching artifact metadata.
For a focused local rebuild before Java tests with `-Dexec.skip=true`:

```bash
python3 dev/native/artifact.py --manifest streamfusion-state-rocksdb/Cargo.toml \
  --target-dir streamfusion-state-rocksdb/target/rust --library libstreamfusion_state_rocksdb.so
python3 dev/native/artifact.py --manifest streamfusion-native/Cargo.toml \
  --target-dir streamfusion-native/target/rust --library libstreamfusion_native.so
```

The wrapper owns compiler-target flags; remove external `RUSTFLAGS`, target overrides or
prebuilt RocksDB/Snappy library overrides. `--cpu x86-64-v2`, `x86-64-v3` and `x86-64-v4`
select portable Linux x86-64 build targets. These are build controls, not runtime options.
Copy a library together with its adjacent `.properties` file when preparing an isolated
benchmark checkout. Mixing metadata from another build fails its checksum check.

This follows Comet's platform-specific `.so`/`.dylib` resource packaging while making
x86 SIMD baselines explicit. These expensive variants run only for a manually invoked
native-release workflow or a published GitHub release, never for ordinary changes.

## Stable component ABI

Rust does not provide a stable ABI, so independently packaged components must not
exchange Rust trait objects, enums, allocator-owned containers, or compiler-specific
layouts across a dynamic-library boundary. StreamFusion will use a versioned C ABI
modeled on the ADBC driver interface:

- Each component exports a well-known `extern "C"` initialization symbol.
- Initialization receives the requested ABI version and fills a versioned function
  table. Unsupported versions are rejected before any execution state is created.
- Component state crosses the boundary only as opaque handles operated on by that
  component's functions.
- Errors use ABI-defined status values and producer-owned error details, never a Rust
  panic or exception across the boundary.
- Creation, cancellation, close, and release functions have explicit ownership and
  idempotency rules.
- The host supplies memory reservation/release callbacks so every component remains
  inside the [Flink-governed memory budget](../memory-and-configuration/).

The ABI definition should live in a small dependency-free interface artifact shared
by the native runtime and optional modules. It must be compatibility-tested against
the oldest supported ABI version whenever its function table grows.

## Direct Arrow handoff

ADBC separates its control API from its data transport: the driver is a C function
table, while result data is returned through the Arrow C Data and C Stream interfaces.
StreamFusion follows the same separation. `ArrowSchema`, `ArrowArray`, and
`ArrowArrayStream` carry schemas, batches, and streams between independently built
native modules. Their producer-provided release callbacks define ownership and allow
buffers to remain reference-counted without copying their contents.

Consequently, a native source, adjacent native operators, and a native sink communicate
directly inside the fused native plan. JNI is used for Flink lifecycle and control
integration at the plan boundary, not as a relay between native components. The host
may wrap or retain an Arrow batch, but it must not serialize, materialize as `RowData`,
or copy the whole batch merely because execution crosses a module boundary.

### Slices at the Java boundary

Arrow's C interfaces can describe arrays with non-zero logical offsets, but Java vector
consumers do not uniformly preserve the semantics of every sliced layout. In particular,
validity bitmaps, variable-width value offsets, and nested child offsets may refer to a
larger parent allocation. A native batch handed to Java must therefore be normalized to
a zero-based Java-safe view. Fixed-width buffers may often remain shared with adjusted
metadata; variable-width and nested offset buffers must be rebased when Java cannot
index them correctly. Only the incompatible metadata or buffers should be copied.

This follows Comet's practical boundary behavior: its Java vectors use Arrow Java
`TransferPair.splitAndTransfer` when creating slices, producing vectors that Java can
consume from index zero before export/import. On native import, Comet also had to align
JVM-produced buffers for types such as `Decimal128`; Arrow 59 incorporates that
realignment into its C Data import path. StreamFusion uses Arrow 59 or newer and still
tests alignment explicitly because allocator and vector layouts remain cross-language
contracts.

The RowData view follows PyFlink's `ArrowReader`: a reusable
`ColumnarRowData` selects a row ID over a `VectorizedColumnBatch`, whose Flink column
vectors directly read Arrow vectors. This avoids allocating a Java row for every
native result. Java exports input and imports output through Arrow C Data structures;
JNI passes only their addresses and the serialized protobuf plan.

The RowData-to-Arrow materializer and Arrow-backed RowData view share this
normalization layer. Tests cover non-zero-offset fixed-width, UTF-8/binary, list, map,
struct, nullable, temporal, and decimal arrays. The end-to-end C Data test also proves
that imported buffers are released by closing the resulting RowData batch view.

ABI-level tests must cover version negotiation, schema compatibility, end-of-stream,
errors, cancellation, and exactly-once release of streams and arrays. Integration tests
must also prove that separately packaged components can be loaded together and exchange
a batch while preserving buffer addresses where the operation itself requires no new
output allocation.
