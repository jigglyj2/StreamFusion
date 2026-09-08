/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowNativeRegionBridge;
import tech.streamfusion.flink.arrow.ArrowNativeRegionOutput;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;

class SharedNativeRegionMetricTest {
    @Test
    void sharedNativeCountsBindOnceToOriginalFlinkScopesIncludingEveryExit() throws Exception {
        var plan = NativeRegionPlan.parseFrom(
                Files.readAllBytes(Path.of("../streamfusion-proto/src/test/resources/native-region-v1.pb")));
        var groups = new HashMap<Long, InterceptingOperatorMetricGroup>();
        var names = new HashMap<Long, String>();
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                long node = ByteBuffer.wrap(id.getBytes()).getLong(8);
                var group = new InterceptingOperatorMetricGroup();
                assertThat(groups.put(node, group)).isNull();
                names.put(node, name);
                return group;
            }
        };
        var memory = TestingNativeMemoryManager.create();
        var type = RowType.of(new IntType());
        try (var allocator = new RootAllocator(64L << 20);
                var context = NativeExecutionContext.region(plan.toByteArray(), memory, null, null);
                var input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(3), GenericRowData.of(19), GenericRowData.of(71)), type, allocator);
                var metrics = StreamFusionNativeMetricTree.forSharedRegion(
                        plan, new OperatorID(0, 0), task, new Configuration(), 0)) {
            var ids = plan.getStagesList().stream()
                    .map(stage -> stage.getOperator().getPlanNodeId())
                    .collect(java.util.stream.Collectors.toList());
            assertThat(groups.keySet()).containsExactlyInAnyOrderElementsOf(ids);
            assertThat(names.values()).containsExactlyInAnyOrder("shared", "branch", "exit");
            var edge = new ArrowNativeRegionBridge(context, List.of(type, type), allocator);
            for (int invocation = 1; invocation <= 3; invocation++) {
                try (var stream = edge.executeStream(List.of(input))) {
                    ArrowNativeRegionOutput.Batch output;
                    while ((output = stream.next()) != null) output.close();
                }
                metrics.update(context);
                for (long id : ids) {
                    var group = groups.get(id);
                    for (String counter : List.of("numRecordsIn", "numRecordsOut", "numBytesIn", "numBytesOut")) {
                        assertThat(group.get(counter)).isInstanceOf(Counter.class);
                        assertThat(group.get(counter + "PerSecond")).isInstanceOf(Meter.class);
                    }
                    assertThat(((Counter) group.get("numRecordsIn")).getCount()).isEqualTo(3L * invocation);
                    assertThat(((Counter) group.get("numRecordsOut")).getCount())
                            .isEqualTo(3L * invocation);
                    assertThat(((Counter) group.get("numBytesOut")).getCount()).isZero();
                    metrics.inputWatermark(id, 0, 100L * invocation);
                    metrics.watermark(id, 100L * invocation);
                    assertThat(((Gauge<?>) group.get("currentInputWatermark")).getValue())
                            .isEqualTo(100L * invocation);
                    assertThat(((Gauge<?>) group.get("currentOutputWatermark")).getValue())
                            .isEqualTo(100L * invocation);
                }
            }
            assertThatThrownBy(metrics::ownerInputRecords).hasMessageContaining("external inputs separately");
            assertThatThrownBy(() -> metrics.update(new long[] {ids.get(0), 9, 18}))
                    .hasMessageContaining("omitted");
            assertThat(((Counter) groups.get(ids.get(0)).get("numRecordsOut")).getCount())
                    .isEqualTo(9);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }
}
