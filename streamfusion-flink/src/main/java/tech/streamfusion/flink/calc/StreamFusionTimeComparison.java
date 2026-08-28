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
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.TimeLiteral;

/** Ordered comparison between a TIME input column and millisecond-of-day literal. */
final class StreamFusionTimeComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final int millisecondOfDay;

    StreamFusionTimeComparison(
            int inputIndex, int millisecondOfDay, int precision, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setTimeLiteral(TimeLiteral.newBuilder()
                                .setMillisecondOfDay(millisecondOfDay)
                                .setPrecision(precision))
                        .build());
        this.millisecondOfDay = millisecondOfDay;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Integer.compare(row.getInt(inputIndex()), millisecondOfDay);
    }
}
