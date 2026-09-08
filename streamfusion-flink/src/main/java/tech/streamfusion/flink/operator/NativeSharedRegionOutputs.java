/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;

/** Flink side outputs broadcast controls, so admitted native exits must share the same frontier. */
final class NativeSharedRegionOutputs {
    private final List<Long> ids;
    private final List<OutputTag<ArrowRowDataBatch>> tags;
    private final Long[] watermarks;
    private final WatermarkStatus[] statuses;

    NativeSharedRegionOutputs(NativeRegionPlan plan, int outputCount) {
        validate(plan, outputCount);
        ids = plan.getOutputStageIdsList();
        tags = java.util.stream.IntStream.range(1, ids.size())
                .mapToObj(NativeSharedRegionOutputs::tag)
                .collect(java.util.stream.Collectors.toUnmodifiableList());
        watermarks = new Long[ids.size()];
        statuses = new WatermarkStatus[ids.size()];
    }

    static void validate(NativeRegionPlan plan, int outputCount) {
        NativeRegionPlanComposer.validate(plan);
        if (outputCount != plan.getOutputStageIdsCount() || outputCount < 2)
            throw new IllegalArgumentException("Shared region output types must match its distinct exits");
        if (plan.getInputCount() != 1)
            throw new IllegalArgumentException("Shared Flink outputs currently require one external input frontier");
        var clocks = new HashMap<Long, List<Long>>();
        for (var stage : plan.getStagesList()) {
            if (stage.getInputsCount() != 1 || stage.getOperator().hasUnion())
                throw new IllegalArgumentException(
                        "Shared Flink outputs require diverging unary stages with identical controls");
            var reference = stage.getInputs(0);
            var path =
                    new ArrayList<Long>(reference.hasExternalInput() ? List.of() : clocks.get(reference.getStageId()));
            // WindowAggOperator can clamp to its restored clock. Every output must pass
            // through exactly the same clock owners; local aggregation does not own a clock.
            if (stage.getOperator().hasWindowAggregate())
                path.add(stage.getOperator().getPlanNodeId());
            clocks.put(stage.getOperator().getPlanNodeId(), List.copyOf(path));
        }
        var expected = clocks.get(plan.getOutputStageIds(0));
        for (long id : plan.getOutputStageIdsList())
            if (!expected.equals(clocks.get(id)))
                throw new IllegalArgumentException("Shared Flink outputs have different restored window-clock paths");
    }

    static OutputTag<ArrowRowDataBatch> tag(int port) {
        if (port <= 0) throw new IllegalArgumentException("The first native region exit uses the main output");
        return new OutputTag<>("streamfusion-region-output-" + port, ArrowRowDataBatchTypeInfo.INSTANCE);
    }

    OutputTag<ArrowRowDataBatch> outputTag(int port) {
        return tags.get(port - 1);
    }

    boolean watermark(long id, long value) {
        int port = ids.indexOf(id);
        return port >= 0 && complete(watermarks, port, value);
    }

    boolean status(long id, WatermarkStatus value) {
        int port = ids.indexOf(id);
        return port >= 0 && complete(statuses, port, value);
    }

    private static <T> boolean complete(T[] pending, int port, T value) {
        if (pending[port] != null)
            throw new IllegalStateException("Native output advanced before all exits completed their control wave");
        pending[port] = value;
        for (T current : pending) if (current == null) return false;
        for (T current : pending)
            if (!Objects.equals(current, value))
                throw new IllegalStateException("Native outputs disagree on their shared Flink control frontier");
        Arrays.fill(pending, null);
        return true;
    }
}
