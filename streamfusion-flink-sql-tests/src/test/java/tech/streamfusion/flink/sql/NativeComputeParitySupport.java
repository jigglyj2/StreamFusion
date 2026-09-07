/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.*;

/** Real Flink SQL is the oracle; direct native handles test retained kernels without changing admission. */
abstract class NativeComputeParitySupport extends SqlParityTestSupport {
    static final RowType INPUT = RowType.of(new IntType(), new BigIntType(), new BigIntType());

    static List<RowData> generated(int seed) {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        for (int i = 0; i < 35; i++)
            rows.add(GenericRowData.of(
                    random.nextInt(4), (long) random.nextInt(7), i % 6 == 0 ? null : (long) random.nextInt(41) - 20));
        return rows;
    }

    static String relation(List<RowData> rows, String alias) {
        var values = new ArrayList<String>();
        for (var row : rows)
            values.add("(CAST(" + (row.isNullAt(0) ? "NULL" : row.getInt(0)) + " AS INT)" + ", CAST(" + row.getLong(1)
                    + " AS BIGINT), CAST(" + (row.isNullAt(2) ? "NULL" : row.getLong(2)) + " AS BIGINT))");
        return "(VALUES " + String.join(",", values) + ") AS " + alias + "(k,o,v)";
    }

    static List<byte[]> flink(String sql, RowType output) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        var environment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        environment
                .getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        var result = new ArrayList<byte[]>();
        try (var rows = environment.executeSql(sql).collect()) {
            while (rows.hasNext()) {
                var row = rows.next();
                var fields = new Object[row.getArity()];
                for (int i = 0; i < fields.length; i++) fields[i] = row.getField(i);
                var value = GenericRowData.of(fields);
                value.setRowKind(row.getKind());
                result.add(bytes(value, output));
            }
        }
        result.sort(Arrays::compareUnsigned);
        return result;
    }

    static byte[] bytes(RowData row, RowType type) throws Exception {
        var output = new DataOutputSerializer(128);
        new RowDataSerializer(type).serialize(row, output);
        return output.getCopyOfBuffer();
    }

    static void append(ArrowRowDataBatch batch, RowType type, List<byte[]> output) throws Exception {
        for (int i = 0; i < batch.size(); i++) output.add(bytes(batch.rowView(i), type));
    }

    static Schema schema(RowType type) {
        var schema = Schema.newBuilder();
        for (var field : type.getFields())
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return schema.build();
    }

    static AggregateCall call(AggregateFunction function, boolean retractable) {
        var call = AggregateCall.newBuilder()
                .setFunction(function)
                .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType()))
                .setRetractable(retractable);
        if (function != AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
            call.setInputIndex(2).setInputType(FlinkLogicalTypeProto.serialize(new BigIntType()));
        return call.build();
    }

    static byte[] plan(Operator.Builder root) {
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(root)
                .build()
                .toByteArray();
    }

    static final class Memory implements NativeMemoryManager {
        long reserved;

        public synchronized boolean tryReserve(long bytes) {
            if (bytes < 0 || bytes > limit() - reserved) return false;
            reserved += bytes;
            return true;
        }

        public synchronized void release(long bytes) {
            if (bytes < 0 || bytes > reserved) throw new IllegalStateException("invalid native release");
            reserved -= bytes;
        }

        public synchronized long available() {
            return limit() - reserved;
        }

        public long limit() {
            return 128L << 20;
        }
    }
}
