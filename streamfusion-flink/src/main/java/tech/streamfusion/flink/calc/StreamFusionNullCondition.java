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
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.NullCheck;

/** SQL {@code IS NULL} or {@code IS NOT NULL} over a direct input reference. */
final class StreamFusionNullCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final Expression expression;

    StreamFusionNullCondition(int inputIndex, boolean negated, Expression operand) {
        this.inputIndex = inputIndex;
        this.expression = Expression.newBuilder()
                .setNullCheck(NullCheck.newBuilder().setOperand(operand).setNegated(negated))
                .build();
    }

    @Override
    public Boolean evaluate(RowData row) {
        boolean isNull = row.isNullAt(inputIndex);
        return expression.getNullCheck().getNegated() ? !isNull : isNull;
    }

    @Override
    public Expression expression() {
        return expression;
    }
}
