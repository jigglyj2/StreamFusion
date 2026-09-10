/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;

/** Flink-generated stages and the ordinary shared native runtime; no test fusion driver. */
final class SharedRegexExtractMetricFixture {
    static final RowType TYPE = RowType.of(new org.apache.flink.table.types.logical.VarCharType(
            org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH));
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RexBuilder rex = new RexBuilder(types);
    private final RexNode id = rex.makeInputRef(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(0)), 0);
    private final List<RexNode> identity = List.of(id);

    private List<RexNode> projects(int stage) {
        if (stage == 1) return identity;
        return List.of(rex.makeCall(
                FlinkSqlOperatorTable.REGEXP_EXTRACT,
                id,
                rex.makeLiteral(stage == 0 ? "(&|^)channel_id=([^&]*)" : "([^&]*)"),
                rex.makeExactLiteral(
                        java.math.BigDecimal.valueOf(stage == 0 ? 2 : 1),
                        types.createFieldTypeFromLogicalType(new IntType()))));
    }

    static long nodeId(int stage) {
        return (1L << 32) | (stage + 1);
    }

    static String uid(int stage) {
        return "shared-regex-metric-stage-" + stage;
    }

    static String name(int stage) {
        return "metric-stage-" + stage;
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    private RexNode condition(int stage) {
        return stage == 1 ? rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, id) : null;
    }

    byte[] plan() {
        var fragments = new ArrayList<byte[]>();
        for (int stage = 0; stage < 3; stage++) {
            String reason = StreamFusionCalcTranslator.unsupportedReason(TYPE, TYPE, projects(stage), condition(stage));
            if (reason != null)
                throw new IllegalArgumentException("stage " + stage + ": " + reason + "; " + projects(stage));
            byte[] fragment = StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, projects(stage), condition(stage));
            fragments.add(
                    StreamFusionNativeRegionTranslator.identifyStage(fragment, stage + 1, name(stage), uid(stage)));
        }
        return StreamFusionNativeRegionTranslator.compose(fragments);
    }

    Oracle oracle(int stage) throws Exception {
        var generator = new CodeGeneratorContext(new Configuration(), getClass().getClassLoader());
        var factory = CalcCodeGenerator.generateCalcOperator(
                generator,
                input(),
                TYPE,
                scala.collection.JavaConverters.asScalaBufferConverter(projects(stage))
                        .asScala()
                        .toSeq(),
                scala.Option.apply(condition(stage)),
                true,
                "RegexMetricStage" + stage);
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0);
        harness.getStreamConfig().setOperatorID(operatorId(stage));
        harness.getStreamConfig().setOperatorName(name(stage));
        harness.setup(new RowDataSerializer(TYPE));
        harness.open();
        return new Oracle(harness);
    }

    static final class Oracle extends FlinkStageMetricOracle {
        Oracle(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
            super(harness);
        }
    }

    static final class NativeHarness extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
        final DataOutputSerializer output = new DataOutputSerializer(128);
        final List<StreamElement> controls = new ArrayList<>();
        int batches;

        NativeHarness(byte[] plan) throws Exception {
            super(
                    new StreamFusionNativeRegionOperatorFactory(List.of(TYPE), TYPE, plan),
                    new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                            .setManagedMemorySize(64L << 20)
                            .build());
            setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
                @Override
                public void collect(StreamRecord<ArrowRowDataBatch> record) {
                    region().getMetricGroup()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .inc();
                    batches++;
                    var batch = record.getValue();
                    for (int row = 0; row < batch.size(); row++) {
                        var value = batch.rowView(row);
                        value.setRowKind(batch.rowKind(row));
                        try {
                            encodeRow(
                                    value,
                                    batch.hasTimestamp(row),
                                    batch.hasTimestamp(row) ? batch.timestamp(row) : 0,
                                    output);
                        } catch (java.io.IOException failure) {
                            throw new java.io.UncheckedIOException(failure);
                        }
                    }
                }
            });
            setup(ArrowRowDataBatchSerializer.INSTANCE);
            open();
        }

        StreamFusionArrowNativeRegionOperator region() {
            return (StreamFusionArrowNativeRegionOperator) operator;
        }

        InternalOperatorMetricGroup stage(int stage) throws Exception {
            return (InternalOperatorMetricGroup) SharedAggregateMetricSurfaceTest.stageGroup(region(), nodeId(stage));
        }

        void accept(ArrowRowDataBatch batch) throws Exception {
            region().getMetricGroup()
                    .getIOMetricGroup()
                    .getNumRecordsInCounter()
                    .inc();
            var input = region().getInputs().get(0);
            var record = new StreamRecord<>(batch);
            input.setKeyContextElement(record);
            input.processElement(record);
        }

        void processWatermark(int port, Watermark mark) throws Exception {
            region().getInputs().get(port).processWatermark(mark);
        }

        void processWatermarkStatus(int port, WatermarkStatus status) throws Exception {
            region().getInputs().get(port).processWatermarkStatus(status);
        }

        @Override
        public void close() throws Exception {
            try {
                super.close();
            } finally {
                getEnvironment().close();
            }
        }

        void latency(LatencyMarker marker) throws Exception {
            region().getInputs().get(0).processLatencyMarker(marker);
        }

        void drainControls() throws Exception {
            for (var control : controls) encode(control, output);
            controls.clear();
        }
    }

    static void encodeRow(RowData row, boolean timestamp, long time, DataOutputSerializer output)
            throws java.io.IOException {
        StageEventBytes.row(TYPE, row, timestamp, time, output);
    }

    static void encode(StreamElement event, DataOutputSerializer output) throws java.io.IOException {
        StageEventBytes.encode(TYPE, event, output);
    }

    private static Transformation<RowData> input() {
        return new Transformation<RowData>("input", InternalTypeInfo.of(TYPE), 1) {
            @Override
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
    }
}
