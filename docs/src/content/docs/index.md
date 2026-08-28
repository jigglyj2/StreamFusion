---
title: StreamFusion
description: A native execution accelerator for Apache Flink SQL.
template: splash
hero:
  tagline: Accelerating Flink SQL execution with Apache DataFusion while preserving Flink's planner, runtime, and fault-tolerance model.
  actions:
    - text: Understand the architecture
      link: /StreamFusion/architecture/
      icon: right-arrow
      variant: primary
    - text: View on GitHub
      link: https://github.com/jigglyj2/StreamFusion
      icon: external
---

StreamFusion is an experimental accelerator for streaming SQL jobs running on Apache Flink. Flink remains responsible for planning, distribution, checkpointing, recovery, and snapshot lifecycle. StreamFusion replaces eligible execution paths with native operators backed by Apache DataFusion.

The project begins with two foundations:

- A SQL parity harness that runs the same streaming jobs through native Flink and the StreamFusion planner extension.
- A reproducible Nexmark benchmark with Kafka input and output, exactly-once delivery, and byte-for-byte result parity as a non-negotiable requirement.

:::caution[Project status]
StreamFusion is early-stage software. The current planner hook and benchmark harness establish the integration boundary; they are not yet a production accelerator.
:::
