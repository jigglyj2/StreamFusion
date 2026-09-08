/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeRegionInputReference;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;
import tech.streamfusion.proto.plan.v1.NativeRegionStage;

/** Versioned DAG composition. Dependencies reference one physical definition, never cloned trees. */
public final class NativeRegionPlanComposer {
    private NativeRegionPlanComposer() {}

    public static byte[] compose(
            int inputCount,
            List<byte[]> fragments,
            List<List<NativeRegionInputReference>> inputs,
            List<Long> outputIds) {
        if (fragments.size() != inputs.size()) throw invalid("stage fragments and bindings must have equal length");
        var region = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(inputCount)
                .addAllOutputStageIds(outputIds);
        for (int index = 0; index < fragments.size(); index++) {
            try {
                var fragment = NativePlan.parseFrom(fragments.get(index));
                if (fragment.getProtocolVersion() < 1 || fragment.getProtocolVersion() > 3 || !fragment.hasRoot())
                    throw invalid("unsupported operator fragment protocol");
                region.addStages(NativeRegionStage.newBuilder()
                        .setOperator(fragment.getRoot())
                        .addAllInputs(inputs.get(index)));
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Invalid native region operator fragment", failure);
            }
        }
        var plan = region.build();
        validate(plan);
        return plan.toByteArray();
    }

    public static void validate(NativeRegionPlan plan) {
        if (plan.getProtocolVersion() != 1) throw invalid("unsupported protocol version");
        if (plan.getInputCount() <= 0 || plan.getStagesCount() == 0 || plan.getOutputStageIdsCount() == 0)
            throw invalid("requires external inputs, stages and outputs");
        Map<Long, NativeRegionStage> seen = new HashMap<>();
        var external = new HashSet<Integer>();
        var uids = new HashSet<String>();
        for (var stage : plan.getStagesList()) {
            if (!stage.hasOperator() || stage.getOperator().getPlanNodeId() <= 0)
                throw invalid("stage identity must be a positive signed 64-bit ID");
            var operator = stage.getOperator();
            long id = operator.getPlanNodeId();
            if (seen.containsKey(id)) throw invalid("duplicate stage identity " + id);
            if (operator.hasMetricUid() && !uids.add(operator.getMetricUid())) throw invalid("duplicate metric UID");
            int arity = NativePlanComposer.inputCount(operator);
            if (arity == 0 || arity != stage.getInputsCount()) throw invalid("stage input arity mismatch");
            for (var child : NativePhysicalPlan.children(operator)) {
                if (child.getPlanNodeId() != 0
                        || !child.getMetricName().isEmpty()
                        || child.hasMetricUid()
                        || child.getClearRecordTimestamps())
                    throw invalid("local Input slots must have no physical identity or record policy");
            }
            for (var input : stage.getInputsList()) {
                switch (input.getSourceCase()) {
                    case EXTERNAL_INPUT:
                        int port = input.getExternalInput();
                        if (port < 0 || port >= plan.getInputCount() || !external.add(port))
                            throw invalid("external input ports must be distinct and in range");
                        break;
                    case STAGE_ID:
                        if (!seen.containsKey(input.getStageId()))
                            throw invalid("stage reference must name an earlier definition");
                        break;
                    default:
                        throw invalid("input reference has no source");
                }
            }
            seen.put(id, stage);
        }
        if (external.size() != plan.getInputCount()) throw invalid("external input port is unused");
        var reachable = new HashSet<Long>();
        var pending = new java.util.ArrayList<Long>();
        for (long id : plan.getOutputStageIdsList()) {
            if (!seen.containsKey(id) || !reachable.add(id))
                throw invalid("output IDs must be distinct stage definitions");
            pending.add(id);
        }
        while (!pending.isEmpty()) {
            for (var input : seen.get(pending.remove(pending.size() - 1)).getInputsList()) {
                if (input.hasStageId() && reachable.add(input.getStageId())) pending.add(input.getStageId());
            }
        }
        if (reachable.size() != seen.size()) throw invalid("stage definition is unreachable from outputs");
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Invalid native region plan: " + reason);
    }
}
