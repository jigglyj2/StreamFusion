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

/** SQL three-valued condition backed directly by a boolean input column. */
final class StreamFusionBooleanColumnCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final Expression expression;

    StreamFusionBooleanColumnCondition(int inputIndex, Expression expression) {
        this.inputIndex = inputIndex;
        this.expression = expression;
    }

    @Override
    public Boolean evaluate(RowData row) {
        return row.isNullAt(inputIndex) ? null : row.getBoolean(inputIndex);
    }

    @Override
    public Expression expression() {
        return expression;
    }
}
