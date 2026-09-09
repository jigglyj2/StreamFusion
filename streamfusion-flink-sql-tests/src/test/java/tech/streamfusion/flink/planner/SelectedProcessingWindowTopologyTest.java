/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeTaskBindings;

/** Ordinary selection must preserve the original owner, buffer share and per-record clock execution. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SelectedProcessingWindowTopologyTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void selectedWindowBindsOriginalResourcesAndExecutesWithTheFlinkClock(boolean rocks) throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            for (int gap : List.of(1000, 10000, 37000)) {
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                System.setProperty(
                        StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY,
                        SelectedLocalWindowSqlProbe.class.getName());
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                var source = env.fromCollection(List.of(Row.of(1L)), Types.ROW_NAMED(new String[] {"k"}, Types.LONG));
                source.getTransformation()
                        .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 11);
                var tables = StreamTableEnvironment.create(env);
                tables.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
                tables.getConfig()
                        .getConfiguration()
                        .set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
                tables.createTemporaryView(
                        "processing_input",
                        source,
                        Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
                var result = tables.toDataStream(
                        tables.sqlQuery("WITH B AS (SELECT k, PROCTIME() AS pt FROM processing_input) "
                                + "SELECT k, COUNT(*) n, window_start, window_end FROM TABLE("
                                + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '" + gap / 1000
                                + "' SECOND)) GROUP BY k, window_start, window_end"));
                var sink =
                        result.addSink(new org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<Row>() {});
                sink.getTransformation().declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 6);
                var original = SelectedLocalWindowSqlProbe.originals.stream()
                        .filter(node -> node.getClass().getSimpleName().equals("StreamExecWindowAggregate"))
                        .collect(java.util.stream.Collectors.toList());
                assertThat(original).hasSize(1);
                var originalWindow = (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate)
                        original.get(0);
                assertThat(StreamFusionSingleStageWindowAdmission.unsupportedReason(originalWindow, tables.getConfig()))
                        .isNull();
                var edges = originalWindow.getInputEdges();
                try {
                    var precedingCalc =
                            edges.get(0).getSource().getInputEdges().get(0).getSource();
                    originalWindow.setInputEdges(
                            List.of(org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.builder()
                                    .source(precedingCalc)
                                    .target(originalWindow)
                                    .build()));
                    assertThat(StreamFusionSingleStageWindowAdmission.unsupportedReason(
                                    originalWindow, tables.getConfig()))
                            .contains("must directly consume an exchange edge");
                } finally {
                    originalWindow.setInputEdges(edges);
                }
                long id = (1L << 32) | Integer.toUnsignedLong(original.get(0).getId());
                assertThat(((ExecNodeBase<?>) original.get(0)).getTransformation())
                        .isNull();
                System.setProperty(
                        StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
                var graph = new StreamGraphGenerator(
                                List.of(sink.getTransformation()),
                                env.getConfig(),
                                env.getCheckpointConfig(),
                                new Configuration())
                        .generate();
                StreamFusionNativeRegionOperatorFactory selected = null;
                for (var node : graph.getStreamNodes()) {
                    if (!(node.getOperatorFactory() instanceof StreamFusionNativeRegionOperatorFactory)) continue;
                    assertThat(node.getTypeSerializerOut()).isSameAs(ArrowRowDataBatchSerializer.INSTANCE);
                    var factory = (StreamFusionNativeRegionOperatorFactory) node.getOperatorFactory();
                    var plan = NativePlan.parseFrom((byte[]) field(factory, "plan"));
                    if (!plan.getRoot().hasWindowAggregate()) continue;
                    assertThat(selected).isNull();
                    selected = InstantiationUtil.clone(factory);
                    assertThat(plan.getRoot().getPlanNodeId()).isEqualTo(id);
                    assertThat(plan.getRoot().getWindowAggregate().getProcessingTime())
                            .isTrue();
                    assertThat(plan.getRoot().getWindowAggregate().getInput().hasInput())
                            .isTrue();
                    assertThat(field(selected, "stateIds")).isEqualTo(List.of(id));
                    var resources = (NativeLocalWindowResources) field(selected, "localWindowResources");
                    assertThat(resources.isPending()).isFalse();
                    try (var environment = new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                            .setManagedMemorySize(64L << 20)
                            .build()) {
                        var config = new StreamConfig(new Configuration());
                        config.setStateBackendUsesManagedMemory(false);
                        var bindings = NativeTaskBindings.parseFrom(resources.resolve(environment, config));
                        assertThat(bindings.getBindingsCount()).isEqualTo(1);
                        assertThat(bindings.getBindings(0).getPlanNodeId()).isEqualTo(id);
                        double share =
                                org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils.getFractionRoundedDown(
                                        1, 18);
                        assertThat(bindings.getBindings(0)
                                        .getLocalWindowBuffer()
                                        .getFlinkBufferMemoryBytes())
                                .isEqualTo(environment.getMemoryManager().computeMemorySize(share));
                    }
                }
                assertThat(selected).isNotNull();
                assertThat(graph.getJobGraph().getNumberOfVertices()).isPositive();
                execute(selected, rocks, gap);
            }
        } finally {
            OriginalMemoryPlanTest.restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
            OriginalMemoryPlanTest.restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    @SuppressWarnings("unchecked")
    private static void execute(StreamFusionNativeRegionOperatorFactory factory, boolean rocks, int gap)
            throws Exception {
        var input = ((List<RowType>) field(factory, "inputTypes")).get(0);
        var output = ((List<RowType>) field(factory, "outputTypes")).get(0);
        var random = new Random(gap);
        try (var flink = ProcessingTimeWindowClockTest.oracle(rocks, gap, null);
                var target = new KeyedNativeMetricHarness(rocks, factory, 1, output, null, 1, 0);
                var allocator = new RootAllocator(64L << 20)) {
            for (int window = 0; window < 3; window++) {
                flink.setProcessingTime((long) window * gap + 1);
                target.setProcessingTime((long) window * gap + 1);
                var serializer = new RowDataSerializer(input);
                for (int batch = 0; batch < 7; batch++) {
                    var rows = new ArrayList<RowData>();
                    for (int i = 0; i < 1 + batch * 3; i++) {
                        var row = GenericRowData.of(i % 3 == 0 ? null : (long) random.nextInt(5), null);
                        rows.add(row);
                        flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
                    }
                    try (var arrow = ArrowRowDataBatch.transpose(rows, input, allocator)) {
                        target.processElement(0, new StreamRecord<>(arrow));
                    }
                }
                flink.setProcessingTime((long) (window + 1) * gap - 1);
                target.setProcessingTime((long) (window + 1) * gap - 1);
                var expected = new DataOutputSerializer(128);
                for (var event : flink.getOutput()) StageEventBytes.encode(output, (StreamElement) event, expected);
                flink.getOutput().clear();
                assertThat(target.maxOutputBatchRows).isPositive();
                assertThat(WindowTimerEventBytes.canonical(output, 3, target.output.getCopyOfBuffer()))
                        .containsExactly(WindowTimerEventBytes.canonical(output, 3, expected.getCopyOfBuffer()));
                target.output.clear();
            }
        }
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }
}
