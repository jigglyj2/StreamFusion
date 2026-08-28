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
import tech.streamfusion.proto.plan.v1.StartsWith;

/** VARCHAR STARTS_WITH condition with a literal prefix. */
final class StreamFusionStartsWithCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final String prefix;
    private final Expression operand;

    StreamFusionStartsWithCondition(int inputIndex, String prefix, Expression operand) {
        this.inputIndex = inputIndex;
        this.prefix = prefix;
        this.operand = operand;
    }

    @Override
    public Boolean evaluate(RowData row) {
        return row.isNullAt(inputIndex)
                ? null
                : row.getString(inputIndex).toString().startsWith(prefix);
    }

    @Override
    public Expression expression() {
        return Expression.newBuilder()
                .setStartsWith(StartsWith.newBuilder().setOperand(operand).setPrefix(prefix))
                .build();
    }
}
