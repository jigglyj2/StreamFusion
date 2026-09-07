/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import com.google.protobuf.Message;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.util.LatencyStats;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Publishes fused physical stages in ordinary Flink operator scopes. */
public final class StreamFusionNativeMetricTree implements AutoCloseable {
    private final Map<Long, Stage> stages = new HashMap<>();
    private final Set<Long> knownNodes = new HashSet<>();
    private final long ownerId;
    private long ownerInput;
    private long ownerOutput;
    private NativeStageGauges gauges;

    public StreamFusionNativeMetricTree(
            byte[] identifiedPlan,
            OperatorID rootId,
            TaskMetricGroup taskMetrics,
            Configuration taskManagerConfig,
            int subtaskIndex) {
        this(identifiedPlan, rootId, taskMetrics, taskManagerConfig, subtaskIndex, 0);
    }

    /** A persistent region's lifecycle owner need not be its output/root Calc stage. */
    public StreamFusionNativeMetricTree(
            byte[] identifiedPlan,
            OperatorID rootId,
            TaskMetricGroup taskMetrics,
            Configuration taskManagerConfig,
            int subtaskIndex,
            long ownerPlanNodeId) {
        this(identifiedPlan, rootId, taskMetrics, taskManagerConfig, subtaskIndex, ownerPlanNodeId, false);
    }

    /** The runtime owner measures region boundaries; every SQL stage, including the root, is separate. */
    public static StreamFusionNativeMetricTree forRegion(
            byte[] identifiedPlan,
            OperatorID runtimeId,
            TaskMetricGroup taskMetrics,
            Configuration taskManagerConfig,
            int subtaskIndex) {
        return new StreamFusionNativeMetricTree(
                identifiedPlan, runtimeId, taskMetrics, taskManagerConfig, subtaskIndex, 0, true);
    }

    private StreamFusionNativeMetricTree(
            byte[] identifiedPlan,
            OperatorID rootId,
            TaskMetricGroup taskMetrics,
            Configuration taskManagerConfig,
            int subtaskIndex,
            long ownerPlanNodeId,
            boolean separateRuntimeOwner) {
        final NativePlan plan;
        try {
            plan = NativePlan.parseFrom(identifiedPlan);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native metric plan", failure);
        }
        if (!plan.hasRoot()) throw new IllegalArgumentException("Native metric plan is missing its root");
        ownerId =
                separateRuntimeOwner ? 0 : ownerPlanNodeId == 0 ? plan.getRoot().getPlanNodeId() : ownerPlanNodeId;
        List<Operator> operators = new ArrayList<>();
        collect(plan, operators);
        int historySize = taskManagerConfig.get(MetricOptions.LATENCY_HISTORY_SIZE);
        if (historySize <= 0) {
            historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
        }
        LatencyStats.Granularity granularity;
        try {
            granularity = LatencyStats.Granularity.valueOf(taskManagerConfig
                    .get(MetricOptions.LATENCY_SOURCE_GRANULARITY)
                    .toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException failure) {
            granularity = LatencyStats.Granularity.OPERATOR;
        }
        Set<String> explicitUids = new HashSet<>();
        for (Operator operator : operators) {
            long id = operator.getPlanNodeId();
            if (id <= 0 || !knownNodes.add(id)) {
                throw new IllegalArgumentException("Missing or duplicate native metric identity: " + id);
            }
            if (operator.hasMetricUid() && operator.getMetricUid().isEmpty()) {
                throw new IllegalArgumentException("Empty string operator uid is not allowed");
            }
            if (operator.hasMetricUid() && !explicitUids.add(operator.getMetricUid())) {
                throw new IllegalArgumentException(
                        "Duplicate Flink operator UID in native metric tree: " + operator.getMetricUid());
            }
            if (id != ownerId
                    && !operator.hasInput()
                    && !operator.hasUnion()
                    && NativePhysicalPlan.children(operator).isEmpty()) {
                throw new IllegalArgumentException("A virtual metric stage requires physical inputs");
            }
        }
        if (!separateRuntimeOwner && !knownNodes.contains(ownerId)) {
            throw new IllegalArgumentException("Unknown native metric lifecycle owner: " + ownerId);
        }
        for (Operator operator : operators) {
            long id = operator.getPlanNodeId();
            // The lifecycle owner uses its real Flink operator's existing metrics. Input is an
            // execution edge, not a virtual Flink operator.
            if (id == ownerId || operator.getOperatorCase() == Operator.OperatorCase.INPUT || operator.hasUnion()) {
                // CommonExecUnion is wiring, not a Flink operator. Keep its native diagnostic
                // counts in the snapshot without inventing a corresponding Flink metric scope.
                continue;
            }
            byte[] identity;
            if (!operator.hasMetricUid()) {
                identity = rootId.getBytes().clone();
                ByteBuffer bytes = ByteBuffer.wrap(identity);
                bytes.putLong(8, bytes.getLong(8) ^ id);
            } else {
                identity = StreamGraphHasherV2.generateUserSpecifiedHash(operator.getMetricUid());
            }
            OperatorID stageId = new OperatorID(identity);
            String name = stageName(operator);
            InternalOperatorMetricGroup group = taskMetrics.getOrAddOperator(stageId, name, Map.of());
            Stage stage = new Stage(
                    group,
                    new LatencyStats(taskMetrics.addGroup("latency"), historySize, subtaskIndex, stageId, granularity),
                    NativePhysicalPlan.children(operator).size());
            group.gauge("currentInputWatermark", new MinWatermarkGauge(stage.inputs));
            group.gauge("currentOutputWatermark", stage.outputWatermark);
            stages.put(id, stage);
        }
    }

    /** Bind native descriptors to stable physical scopes, without operator-family dispatch. */
    public void bindGauges(byte[] schema, java.util.function.Supplier<long[]> snapshot) {
        if (gauges != null) throw new IllegalStateException("Native gauges are already bound");
        gauges = new NativeStageGauges(
                schema,
                id -> {
                    Stage stage = stages.get(id);
                    return stage == null ? null : stage.group;
                },
                snapshot);
    }

    public void update(NativeExecutionContext context) {
        update(context.metricSnapshot());
        if (gauges != null) gauges.update();
    }

    public void updateAfterFailure(NativeExecutionContext context, Throwable executionFailure) {
        updateAfterFailure(context::metricSnapshot, executionFailure);
        try {
            if (gauges != null) gauges.update();
        } catch (RuntimeException | Error metricFailure) {
            if (metricFailure != executionFailure) executionFailure.addSuppressed(metricFailure);
        }
    }

    public void update(long[] snapshot) {
        if (snapshot.length % 3 != 0) {
            throw new IllegalArgumentException("Native metric snapshot has an invalid shape");
        }
        Set<Long> seen = new HashSet<>();
        for (int index = 0; index < snapshot.length; index += 3) {
            long id = snapshot[index];
            if (!knownNodes.contains(id) || !seen.add(id) || snapshot[index + 1] < 0 || snapshot[index + 2] < 0) {
                throw new IllegalArgumentException("Invalid native metric snapshot at plan node " + id);
            }
            Stage stage = stages.get(id);
            if (id == ownerId && (snapshot[index + 1] < ownerInput || snapshot[index + 2] < ownerOutput)) {
                throw new IllegalArgumentException("Native metric counters decreased at lifecycle owner " + id);
            }
            if (stage != null && (snapshot[index + 1] < stage.input || snapshot[index + 2] < stage.output)) {
                throw new IllegalArgumentException("Native metric counters decreased at plan node " + id);
            }
        }
        if (!seen.equals(knownNodes)) {
            throw new IllegalArgumentException("Native metric snapshot omitted a plan node");
        }
        for (int index = 0; index < snapshot.length; index += 3) {
            if (snapshot[index] == ownerId) {
                ownerInput = snapshot[index + 1];
                ownerOutput = snapshot[index + 2];
            }
            Stage stage = stages.get(snapshot[index]);
            if (stage != null) {
                var io = stage.group.getIOMetricGroup();
                io.getNumRecordsInCounter().inc(snapshot[index + 1] - stage.input);
                io.getNumRecordsOutCounter().inc(snapshot[index + 2] - stage.output);
                stage.input = snapshot[index + 1];
                stage.output = snapshot[index + 2];
            }
        }
    }

    /** Failure must not erase already consumed stage records or replace the execution exception. */
    public void updateAfterFailure(java.util.function.Supplier<long[]> snapshot, Throwable executionFailure) {
        try {
            long[] values = snapshot.get();
            // Lowering may fail before a physical metric tree has been installed.
            if (values.length != 0) update(values);
        } catch (RuntimeException | Error metricFailure) {
            if (metricFailure != executionFailure) executionFailure.addSuppressed(metricFailure);
        }
    }

    /** Finds a lifecycle owner by identity without assuming which operator families surround it. */
    public static long uniqueNodeId(byte[] identifiedPlan, Operator.OperatorCase kind) {
        final NativePlan plan;
        try {
            plan = NativePlan.parseFrom(identifiedPlan);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native metric plan", failure);
        }
        List<Operator> operators = new ArrayList<>();
        collect(plan, operators);
        List<Operator> owners = operators.stream()
                .filter(node -> node.getOperatorCase() == kind)
                .collect(java.util.stream.Collectors.toList());
        if (owners.size() != 1 || owners.get(0).getPlanNodeId() <= 0) {
            throw new IllegalArgumentException("Native lifecycle requires exactly one identified " + kind + " owner");
        }
        return owners.get(0).getPlanNodeId();
    }

    public void watermark(long timestamp) {
        // Compatibility unary tails receive the same watermark at each stage. The general
        // tree reports each physical input separately through inputWatermark instead.
        stages.values().forEach(stage -> {
            for (WatermarkGauge input : stage.inputs) input.setCurrentWatermark(Math.max(input.getValue(), timestamp));
            stage.outputWatermark.setCurrentWatermark(Math.max(stage.outputWatermark.getValue(), timestamp));
        });
    }

    public void inputWatermark(long nodeId, int port, long timestamp) {
        requireNode(nodeId);
        Stage stage = stages.get(nodeId);
        if (stage != null) {
            stage.inputs[java.util.Objects.checkIndex(port, stage.inputs.length)].setCurrentWatermark(timestamp);
        }
    }

    public void watermark(long nodeId, long timestamp) {
        requireNode(nodeId);
        Stage stage = stages.get(nodeId);
        if (stage != null) {
            stage.outputWatermark.setCurrentWatermark(timestamp);
        }
    }

    public long ownerInputRecords() {
        if (ownerId == 0) throw new IllegalStateException("A region runtime must count its external inputs separately");
        return ownerInput;
    }

    public void latency(long nodeId, LatencyMarker marker) {
        requireNode(nodeId);
        Stage stage = stages.get(nodeId);
        if (stage != null) {
            stage.latency.reportLatency(marker);
        }
    }

    private void requireNode(long nodeId) {
        if (!knownNodes.contains(nodeId)) {
            throw new IllegalArgumentException("Unknown native metric stage: " + nodeId);
        }
    }

    public void latency(LatencyMarker marker) {
        stages.values().forEach(stage -> stage.latency.reportLatency(marker));
    }

    @Override
    public void close() {
        stages.values().forEach(stage -> stage.group.close());
    }

    private static String stageName(Operator operator) {
        if (!operator.getMetricName().isEmpty()) return operator.getMetricName();
        // Compatibility callers without a selected Flink graph have no original physical label.
        switch (operator.getOperatorCase()) {
            case CALC:
                return "Calc";
            case ARRAY_UNNEST:
            case REPLICATE_ROWS:
                return "Correlate";
            case EXPAND:
                return "Expand";
            default:
                return operator.getOperatorCase().name();
        }
    }

    private static void collect(Message message, List<Operator> operators) {
        if (message instanceof Operator) {
            operators.add((Operator) message);
        }
        for (Object value : message.getAllFields().values()) {
            if (value instanceof Message) {
                collect((Message) value, operators);
            } else if (value instanceof Iterable<?>) {
                for (Object item : (Iterable<?>) value) {
                    if (item instanceof Message) {
                        collect((Message) item, operators);
                    }
                }
            }
        }
    }

    private static final class Stage {
        private final InternalOperatorMetricGroup group;
        private final LatencyStats latency;
        private long input;
        private long output;
        private final WatermarkGauge[] inputs;
        private final WatermarkGauge outputWatermark = new WatermarkGauge();

        private Stage(InternalOperatorMetricGroup group, LatencyStats latency, int arity) {
            this.group = group;
            this.latency = latency;
            if (arity <= 0) throw new IllegalArgumentException("A virtual metric stage requires physical inputs");
            inputs = new WatermarkGauge[arity];
            java.util.Arrays.setAll(inputs, ignored -> new WatermarkGauge());
        }
    }
}
