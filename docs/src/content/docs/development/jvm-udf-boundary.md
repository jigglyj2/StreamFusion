---
title: JVM UDF boundary proposal
description: Proposed Arrow-batch callback contract for existing Flink scalar UDFs; not implemented or approved.
---

**Status: proposal only.** Java scalar UDFs currently retain whole-plan fallback. Implementing
this proposal requires an explicit exception to the repository's rule that JNI crossings occur
only at the outer edges of a fused native plan. No such exception is assumed here.

Q14 uses the upstream Java `CountChar` UDF. It counts occurrences of the first UTF-8 byte of its
second argument, returns zero for a null first argument, and can throw for a null or empty second
argument. Replacing it with a similarly named native character-count function would neither
preserve arbitrary user code nor be a general accelerator feature.

## Reference and proposed contract

The clean Comet reference at `4897161704b7b8b7dfa909f4bf897c6508b11117` supports JVM scalar UDFs
through `CometScalaUDFCodegen` and `CometUdfBridge`. Native execution passes Arrow vectors to a
compiled JVM batch kernel and receives an Arrow result. Task-scoped instances and the original
user classloader preserve UDF identity across callbacks. This is a scalar-expression callback,
not an intermediate Spark row operator.

The corresponding StreamFusion contract would be:

- Represent a JVM UDF call as a DataFusion physical scalar expression inside the existing native
  plan. Serialize a versioned function identity and typed argument/result contract in protobuf;
  retain the original Flink function description and generated evaluator on the Java planner side.
- Cross JNI once per evaluated argument batch, using Arrow C Data ownership and release callbacks.
  Read arguments from Arrow vectors and write the result vector directly. Do not serialize full
  batches, construct intermediate RowData batches, or make JNI calls per row.
- Invoke the actual Flink UDF through Flink's code generation and type conversions. Retain its
  null, exception, argument order, return-type, open/close and task/classloader semantics. Ordinary
  scalar user code may still run once per row within the JVM batch kernel.
- Keep adjacent native operators composed and independently observable. Flink remains responsible
  for planning, checkpoints, distribution and recovery. Any changed evaluation ordering or UDF
  lifecycle that cannot be proven equivalent receives precise whole-plan fallback.
- Allocate large Arrow outputs and growing callback workspace through the existing Flink-managed
  allowance, count shared buffers once, and close resources on success, cancellation and failure.
  Do not add a global allocator policy or a separate runtime memory setting.
- Admit only function/type/lifecycle subsets with generated full-changelog, metric, ownership,
  exception and task-recovery parity. Reject unresolved user classes and unsupported semantics.
  Do not recognize Nexmark UDF class names or translate their implementations specially.

This would be a narrow exception for execution of existing JVM UDFs. It would not authorize
ordinary Flink intermediate operators, native-to-native Java handoffs, per-row JNI, arbitrary
upstream forks, or migration of built-in DataFusion computation back to Java. The initial work
should use one task-thread callback at a time; reproducing Spark's worker migration behavior is
unnecessary for Flink's synchronous mailbox lifecycle.

## Independent Q14 prerequisites

The timestamp `EXTRACT` and decimal-expression portions can be investigated and implemented
under the existing native-only rules. Q14 must retain whole-plan fallback until every blocker,
including its original UDF, is resolved within the approved architecture.
