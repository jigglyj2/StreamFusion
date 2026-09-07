/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.TruthTest;
import tech.streamfusion.proto.plan.v1.TruthTestOperator;

/** Calcite Sarg null policy, applied after constructing its three-valued range predicate. */
final class StreamFusionSearchNullSemantics {
    private StreamFusionSearchNullSemantics() {}

    static boolean supports(String nullAs) {
        return "UNKNOWN".equals(nullAs) || "TRUE".equals(nullAs) || "FALSE".equals(nullAs);
    }

    static Expression apply(Expression ranges, String nullAs) {
        if (ranges == null || !supports(nullAs)) return null;
        if ("UNKNOWN".equals(nullAs)) return ranges;
        // IS NOT FALSE maps only UNKNOWN to TRUE; IS TRUE maps only UNKNOWN to FALSE.
        // Unlike OR/AND with another null check, this evaluates the range expression once.
        return Expression.newBuilder()
                .setTruthTest(TruthTest.newBuilder()
                        .setOperand(ranges)
                        .setOperator(
                                "TRUE".equals(nullAs)
                                        ? TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_FALSE
                                        : TruthTestOperator.TRUTH_TEST_OPERATOR_IS_TRUE))
                .build();
    }
}
