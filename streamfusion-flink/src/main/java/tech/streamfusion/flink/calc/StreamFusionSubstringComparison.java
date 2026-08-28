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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StringLiteral;

/** Ordered comparison between a safe SUBSTRING expression and a VARCHAR literal. */
final class StreamFusionSubstringComparison extends StreamFusionOrderedComparison {
    private static final long serialVersionUID = 1L;
    private final String literal;
    private final int start;
    private final int length;
    private transient BinaryStringData literalData;

    StreamFusionSubstringComparison(
            int inputIndex,
            int start,
            int length,
            String literal,
            ComparisonOperator operator,
            boolean substringOnLeft,
            Expression substringExpression) {
        super(
                inputIndex,
                operator,
                substringOnLeft,
                Expression.newBuilder()
                        .setStringLiteral(StringLiteral.newBuilder().setValue(literal))
                        .build(),
                substringExpression);
        this.start = start;
        this.length = length;
        this.literal = literal;
    }

    @Override
    protected int compareInputToLiteral(RowData row) {
        if (literalData == null) {
            literalData = BinaryStringData.fromString(literal);
        }
        BinaryStringData value =
                BinaryStringDataUtil.substringSQL((BinaryStringData) row.getString(inputIndex()), start, length);
        return value.compareTo(literalData);
    }
}
