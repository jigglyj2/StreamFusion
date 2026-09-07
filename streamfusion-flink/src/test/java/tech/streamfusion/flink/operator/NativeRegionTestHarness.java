/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

/** Copies only at the test sink, while the native output is still borrowed and alive. */
final class NativeRegionTestHarness extends MultiInputStreamOperatorTestHarness<ArrowRowDataBatch> {
    final List<RowData> rows = new ArrayList<>();
    final List<StreamElement> controls = new ArrayList<>();
    RuntimeException sinkFailure;

    NativeRegionTestHarness(byte[] plan, List<RowType> inputTypes, RowType outputType) throws Exception {
        super(new StreamFusionNativeRegionOperatorFactory(inputTypes, outputType, plan));
        var serializer = new RowDataSerializer(outputType);
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                metrics().getIOMetricGroup().getNumRecordsOutCounter().inc();
                if (sinkFailure != null) throw sinkFailure;
                var batch = record.getValue();
                for (int row = 0; row < batch.size(); row++) {
                    RowData copy = serializer.copy(batch.rowView(row));
                    copy.setRowKind(batch.rowKind(row));
                    rows.add(copy);
                }
            }
        });
        setup(ArrowRowDataBatchSerializer.INSTANCE);
    }

    void accept(int input, ArrowRowDataBatch batch) throws Exception {
        metrics().getIOMetricGroup().getNumRecordsInCounter().inc();
        processElement(input, new StreamRecord<>(batch));
    }

    OperatorMetricGroup metrics() {
        return getCastedOperator().getMetricGroup();
    }

    OperatorMetricGroup stageMetrics(long id) throws Exception {
        return stageMetrics(getCastedOperator(), id);
    }

    static OperatorMetricGroup stageMetrics(Object operator, long id) throws Exception {
        var treeField = operator.getClass().getDeclaredField("metricTree");
        treeField.setAccessible(true);
        Object tree = treeField.get(operator);
        var stagesField = tree.getClass().getDeclaredField("stages");
        stagesField.setAccessible(true);
        Object stage = ((java.util.Map<?, ?>) stagesField.get(tree)).get(id);
        if (stage == null) throw new AssertionError("No independently observable metric stage " + id);
        var groupField = stage.getClass().getDeclaredField("group");
        groupField.setAccessible(true);
        return (OperatorMetricGroup) groupField.get(stage);
    }

    void end(int input) throws Exception {
        ((StreamFusionArrowNativeRegionOperator) getCastedOperator()).endInput(input + 1);
    }
}
