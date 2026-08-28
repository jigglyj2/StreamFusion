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

import java.math.BigDecimal;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.Expression;

/** Ordered comparison between a DECIMAL input column and same-scale literal. */
final class StreamFusionDecimalComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final BigDecimal literal;
    private final int precision;
    private final int scale;

    StreamFusionDecimalComparison(
            int inputIndex,
            BigDecimal literal,
            int precision,
            int scale,
            ComparisonOperator operator,
            boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setDecimalLiteral(DecimalLiteral.newBuilder()
                                .setUnscaledValue(literal.unscaledValue().toString())
                                .setPrecision(precision)
                                .setScale(scale))
                        .build());
        this.literal = literal;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return row.getDecimal(inputIndex(), precision, scale).toBigDecimal().compareTo(literal);
    }
}
