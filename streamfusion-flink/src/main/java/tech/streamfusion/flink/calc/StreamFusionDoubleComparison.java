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
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.Expression;

/** Ordered comparison between a finite DOUBLE input column and literal. */
final class StreamFusionDoubleComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final double literal;

    StreamFusionDoubleComparison(int inputIndex, double literal, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setDoubleLiteral(DoubleLiteral.newBuilder().setValue(literal))
                        .build());
        this.literal = literal;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Double.compare(row.getDouble(inputIndex()), literal);
    }
}
