/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;

/** Ordered comparison between a local TIMESTAMP input column and literal. */
final class StreamFusionTimestampComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final long epochMillisecond;
    private final int nanoOfMillisecond;
    private final int precision;

    StreamFusionTimestampComparison(
            int inputIndex, TimestampData literal, int precision, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setTimestampLiteral(TimestampLiteral.newBuilder()
                                .setEpochMillisecond(literal.getMillisecond())
                                .setNanoOfMillisecond(literal.getNanoOfMillisecond())
                                .setPrecision(precision))
                        .build());
        this.epochMillisecond = literal.getMillisecond();
        this.nanoOfMillisecond = literal.getNanoOfMillisecond();
        this.precision = precision;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        TimestampData input = row.getTimestamp(inputIndex(), precision);
        int millisComparison = Long.compare(input.getMillisecond(), epochMillisecond);
        return millisComparison != 0
                ? millisComparison
                : Integer.compare(input.getNanoOfMillisecond(), nanoOfMillisecond);
    }
}
