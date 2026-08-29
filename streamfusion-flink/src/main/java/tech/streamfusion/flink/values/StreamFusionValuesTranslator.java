/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.values;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.Values;
import tech.streamfusion.proto.plan.v1.ValuesRow;

/** Reflection entry point used by the planner extension for source-free native VALUES. */
public final class StreamFusionValuesTranslator {
    private static final RowType EMPTY_INPUT = RowType.of(new LogicalType[0]);

    private StreamFusionValuesTranslator() {}

    public static Transformation<RowData> translate(
            StreamExecutionEnvironment environment, RowType outputType, List<List<?>> tuples) {
        if (unsupportedReason(outputType, tuples) != null) {
            return null;
        }
        StreamFusionValuesInputFormat inputFormat =
                new StreamFusionValuesInputFormat(createPlan(outputType, tuples), outputType);
        Transformation<RowData> transformation = environment
                .createInput(inputFormat, InternalTypeInfo.of(outputType))
                .getTransformation();
        transformation.setName("streamfusion-values");
        transformation.setDescription("StreamFusionValues");
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        return transformation;
    }

    public static String unsupportedReason(RowType outputType, List<List<?>> tuples) {
        for (int field = 0; field < outputType.getFieldCount(); field++) {
            try {
                StreamFusionCalcTranslator.operatorLogicalType(outputType.getTypeAt(field));
            } catch (IllegalArgumentException error) {
                return "fields[" + field + "]: " + error.getMessage();
            }
        }
        for (int row = 0; row < tuples.size(); row++) {
            String reason =
                    StreamFusionCalcTranslator.unsupportedReason(EMPTY_INPUT, outputType, tuples.get(row), null);
            if (reason != null) {
                return "rows[" + row + "]/" + reason;
            }
        }
        return null;
    }

    static byte[] createPlan(RowType outputType, List<List<?>> tuples) {
        Schema.Builder schema = Schema.newBuilder();
        for (int field = 0; field < outputType.getFieldCount(); field++) {
            schema.addFields(Field.newBuilder()
                    .setName(outputType.getFieldNames().get(field))
                    .setType(StreamFusionCalcTranslator.operatorLogicalType(outputType.getTypeAt(field))));
        }
        Values.Builder values = Values.newBuilder().setSchema(schema);
        for (List<?> tuple : tuples) {
            ValuesRow.Builder row = ValuesRow.newBuilder();
            for (int field = 0; field < tuple.size(); field++) {
                Expression expression = StreamFusionCalcTranslator.operatorExpression(
                        tuple.get(field), EMPTY_INPUT, outputType.getTypeAt(field));
                row.addValues(expression);
            }
            values.addRows(row);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setValues(values))
                .build()
                .toByteArray();
    }
}
