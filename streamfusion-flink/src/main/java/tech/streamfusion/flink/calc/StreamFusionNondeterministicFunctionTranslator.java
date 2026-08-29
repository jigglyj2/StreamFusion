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

/** Explicit parity decisions for scalar functions whose value depends on execution history. */
final class StreamFusionNondeterministicFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionNondeterministicFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("UUID".equals(function)) {
            return "UUID stays on Flink because each invocation uses the JVM secure random UUID generator and a native invocation cannot reproduce the same values byte-for-byte";
        }
        if ("RAND".equals(function) || "RAND_INTEGER".equals(function)) {
            return function
                    + " stays on Flink because Flink's Java random-number state advances per row and function instance; native batch evaluation has no parity-proven mapping for that lifecycle, including seeded calls";
        }
        return null;
    }
}
