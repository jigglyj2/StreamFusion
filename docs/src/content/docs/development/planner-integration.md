---
title: Planner integration
description: The minimal hook used to select StreamFusion planning logic.
---

StreamFusion starts with a narrowly scoped patch to the Flink table planner. The patch permits a planner factory implementation to be selected without forking the rest of Flink's SQL stack.

The `streamfusion-flink` module supplies the planner-side integration under `tech.streamfusion.flink`. Tests can select the StreamFusion implementation for one execution and clear that selection for the native Flink baseline.

Keeping the hook small matters: the target is to follow Flink's architecture and release line closely, not maintain a broad planner fork. Changes to the upstream patch should therefore be isolated, tested by the SQL harness, and reviewed independently from native operator work.
