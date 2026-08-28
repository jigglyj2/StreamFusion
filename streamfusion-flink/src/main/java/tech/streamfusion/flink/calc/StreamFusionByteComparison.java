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
import tech.streamfusion.proto.plan.v1.ByteLiteral;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Ordered comparison between a TINYINT input column and literal. */
final class StreamFusionByteComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final byte literal;

    StreamFusionByteComparison(int inputIndex, byte literal, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setByteLiteral(ByteLiteral.newBuilder().setValue(literal))
                        .build());
        this.literal = literal;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Byte.compare(row.getByte(inputIndex()), literal);
    }
}
