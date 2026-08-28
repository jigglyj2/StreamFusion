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
import tech.streamfusion.proto.plan.v1.DateLiteral;
import tech.streamfusion.proto.plan.v1.Expression;

/** Ordered comparison between a DATE input column and an epoch-day literal. */
final class StreamFusionDateComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final int epochDay;

    StreamFusionDateComparison(int inputIndex, int epochDay, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setDateLiteral(DateLiteral.newBuilder().setEpochDay(epochDay))
                        .build());
        this.epochDay = epochDay;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Integer.compare(row.getInt(inputIndex()), epochDay);
    }
}
