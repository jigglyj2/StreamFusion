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

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.proto.plan.v1.BinaryLiteral;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Ordered comparison between a BINARY or VARBINARY input column and literal. */
final class StreamFusionBinaryComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final byte[] literal;

    StreamFusionBinaryComparison(int inputIndex, byte[] literal, ComparisonOperator operator, boolean inputOnLeft) {
        this(inputIndex, literal, false, literal.length, operator, inputOnLeft);
    }

    StreamFusionBinaryComparison(
            int inputIndex,
            byte[] literal,
            boolean fixedWidth,
            int length,
            ComparisonOperator operator,
            boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setBinaryLiteral(BinaryLiteral.newBuilder()
                                .setValue(ByteString.copyFrom(literal))
                                .setFixedWidth(fixedWidth)
                                .setLength(length))
                        .build());
        if (fixedWidth && literal.length != length) {
            throw new IllegalArgumentException("BINARY(" + length + ") literal has " + literal.length + " bytes");
        }
        this.literal = literal.clone();
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Arrays.compareUnsigned(row.getBinary(inputIndex()), literal);
    }
}
