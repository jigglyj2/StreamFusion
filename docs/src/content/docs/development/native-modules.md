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
  inside the [Flink-governed memory budget](./memory-and-configuration/).

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

The RowData-to-Arrow materializer and Arrow-backed RowData view must share this
normalization layer. Tests cover non-zero-offset fixed-width, UTF-8/binary, list, map,
struct, nullable, dictionary, and decimal arrays, and verify that every producer release
callback runs exactly once.

ABI-level tests must cover version negotiation, schema compatibility, end-of-stream,
errors, cancellation, and exactly-once release of streams and arrays. Integration tests
must also prove that separately packaged components can be loaded together and exchange
a batch while preserving buffer addresses where the operation itself requires no new
output allocation.
