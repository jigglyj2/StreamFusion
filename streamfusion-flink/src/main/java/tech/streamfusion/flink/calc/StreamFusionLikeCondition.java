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
import org.apache.flink.table.functions.SqlLikeUtils;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Like;

/** SQL LIKE condition with a literal pattern and no explicit escape clause. */
final class StreamFusionLikeCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final String pattern;
    private final Expression operand;

    StreamFusionLikeCondition(int inputIndex, String pattern, Expression operand) {
        this.inputIndex = inputIndex;
        this.pattern = pattern;
        this.operand = operand;
    }

    @Override
    public Boolean evaluate(RowData row) {
        return row.isNullAt(inputIndex)
                ? null
                : SqlLikeUtils.like(row.getString(inputIndex).toString(), pattern);
    }

    @Override
    public Expression expression() {
        return Expression.newBuilder()
                .setLike(Like.newBuilder().setOperand(operand).setPattern(pattern))
                .build();
    }
}
