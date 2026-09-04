package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Result sink that serializes the complete Flink changelog for deterministic comparison. */
final class BenchmarkResultDynamicTableSink implements DynamicTableSink {
    private final DataType consumedType;
    private final String runId;

    BenchmarkResultDynamicTableSink(DataType consumedType, String runId) {
        this.consumedType = consumedType;
        this.runId = runId;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) consumedType.getLogicalType();
        TypeSerializer<RowData> serializer = InternalTypeInfo.of(rowType).createSerializer(new SerializerConfigImpl());
        return SinkFunctionProvider.of(new ResultSinkFunction(runId, rowType, serializer));
    }

    @Override
    public DynamicTableSink copy() {
        return new BenchmarkResultDynamicTableSink(consumedType, runId);
    }

    @Override
    public String asSummaryString() {
        return "StreamFusion benchmark result sink";
    }

    private static final class ResultSinkFunction extends RichSinkFunction<RowData> {
        private final String runId;
        private final RowType rowType;
        private final TypeSerializer<RowData> serializer;
        private transient RowData.FieldGetter[] fieldGetters;

        private ResultSinkFunction(String runId, RowType rowType, TypeSerializer<RowData> serializer) {
            this.runId = runId;
            this.rowType = rowType;
            this.serializer = serializer;
        }

        @Override
        public void open(OpenContext context) {
            fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
            for (int index = 0; index < fieldGetters.length; index++) {
                fieldGetters[index] = RowData.createFieldGetter(rowType.getTypeAt(index), index);
            }
        }

        @Override
        public void invoke(RowData value, Context context) throws IOException {
            DataOutputSerializer output = new DataOutputSerializer(256);
            serializer.serialize(value, output);
            RowData materialized = serializer.copy(value);
            boolean accumulate = value.getRowKind() == RowKind.INSERT || value.getRowKind() == RowKind.UPDATE_AFTER;
            materialized.setRowKind(RowKind.INSERT);
            DataOutputSerializer materializedOutput = new DataOutputSerializer(256);
            serializer.serialize(materialized, materializedOutput);
            Object[] fields = new Object[fieldGetters.length];
            for (int index = 0; index < fieldGetters.length; index++) {
                fields[index] = fieldGetters[index].getFieldOrNull(value);
            }
            BenchmarkResultStore.add(
                    runId,
                    output.getCopyOfBuffer(),
                    materializedOutput.getCopyOfBuffer(),
                    accumulate,
                    value.getRowKind().shortString() + Arrays.toString(fields),
                    RowKind.INSERT.shortString() + Arrays.toString(fields));
        }
    }
}
