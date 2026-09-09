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
    final List<RowData> rows;
    final List<List<RowData>> outputRows = new ArrayList<>();
    final List<List<Long>> outputTimes = new ArrayList<>();
    final List<StreamElement> controls = new ArrayList<>();
    RuntimeException sinkFailure;
    boolean cancelOnClose;

    NativeRegionTestHarness(byte[] plan, List<RowType> inputTypes, RowType outputType) throws Exception {
        this(new StreamFusionNativeRegionOperatorFactory(inputTypes, outputType, plan), List.of(outputType));
    }

    NativeRegionTestHarness(StreamFusionNativeRegionOperatorFactory factory, List<RowType> outputTypes)
            throws Exception {
        super(factory);
        var serializers =
                outputTypes.stream().map(RowDataSerializer::new).collect(java.util.stream.Collectors.toList());
        for (int port = 0; port < outputTypes.size(); port++) {
            outputRows.add(new ArrayList<>());
            outputTimes.add(new ArrayList<>());
        }
        rows = outputRows.get(0);
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                capture(0, record.getValue());
            }

            @Override
            public <X> void collect(org.apache.flink.util.OutputTag<X> tag, StreamRecord<X> record) {
                for (int port = 1; port < outputTypes.size(); port++)
                    if (factory.outputTag(port).equals(tag)) {
                        capture(port, (ArrowRowDataBatch) record.getValue());
                        return;
                    }
                throw new AssertionError("Unknown region output " + tag);
            }

            private void capture(int port, ArrowRowDataBatch batch) {
                metrics().getIOMetricGroup().getNumRecordsOutCounter().inc();
                if (sinkFailure != null) throw sinkFailure;
                for (int row = 0; row < batch.size(); row++) {
                    RowData copy = serializers.get(port).copy(batch.rowView(row));
                    copy.setRowKind(batch.rowKind(row));
                    outputRows.get(port).add(copy);
                    outputTimes.get(port).add(batch.hasTimestamp(row) ? batch.timestamp(row) : null);
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

    StreamFusionArrowNativeRegionOperator region() {
        return (StreamFusionArrowNativeRegionOperator) getCastedOperator();
    }

    void end(int input) throws Exception {
        ((StreamFusionArrowNativeRegionOperator) getCastedOperator()).endInput(input + 1);
    }

    @Override
    public void close() throws Exception {
        if (!cancelOnClose) {
            super.close();
            return;
        }
        // Flink skips finish on task cancellation. The upstream harness unconditionally
        // calls finish before close, which would issue new controls to a failed invocation.
        processingTimeService.shutdownService();
        try {
            getCastedOperator().close();
        } finally {
            try {
                getEnvironment().close();
            } finally {
                var cleanup = org.apache.flink.streaming.util.MockStreamTask.class.getDeclaredMethod("cleanUpInternal");
                cleanup.setAccessible(true);
                cleanup.invoke(mockTask);
            }
        }
    }
}
