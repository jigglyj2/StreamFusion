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
import tech.streamfusion.proto.plan.v1.BooleanLiteral;
import tech.streamfusion.proto.plan.v1.Expression;

/** A constant boolean used in a projected or filtering boolean expression. */
final class StreamFusionBooleanLiteralCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;

    private final boolean value;

    StreamFusionBooleanLiteralCondition(boolean value) {
        this.value = value;
    }

    @Override
    public Boolean evaluate(RowData row) {
        return value;
    }

    @Override
    public Expression expression() {
        return Expression.newBuilder()
                .setBooleanLiteral(BooleanLiteral.newBuilder().setValue(value))
                .build();
    }
}
