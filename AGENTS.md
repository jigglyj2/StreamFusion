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

## Testing Guidelines

Exact byte to byte parity with Flink's result set is paramount. Add our own tests to ensure this, use normal Flink processing when we can't achieve it. Hook into existing Flink SQL targets where possible.

The Kafka-in/Kafka-out, exactly-once Nexmark comparison against unmodified Flink is
our north-star benchmark. Optimize its four state-backend/mini-batch cases while
keeping the code simple and avoiding substantial divergence from Flink's result
parity and architecture.

Immediately before each commit, run Palantir Java formatting and only the unit tests
relevant to that commit. Treat this like a focused commit hook; do not spend time
running unrelated test suites.
