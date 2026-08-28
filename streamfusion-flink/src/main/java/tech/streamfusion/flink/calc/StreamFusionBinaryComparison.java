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

/** Ordered comparison between a VARBINARY input column and literal. */
final class StreamFusionBinaryComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final byte[] literal;

    StreamFusionBinaryComparison(int inputIndex, byte[] literal, ComparisonOperator operator, boolean inputOnLeft) {
        super(
                inputIndex,
                operator,
                inputOnLeft,
                Expression.newBuilder()
                        .setBinaryLiteral(BinaryLiteral.newBuilder()
                                .setValue(ByteString.copyFrom(literal))
                                .setFixedWidth(false)
                                .setLength(literal.length))
                        .build());
        this.literal = literal.clone();
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        return Arrays.compareUnsigned(row.getBinary(inputIndex()), literal);
    }
}
