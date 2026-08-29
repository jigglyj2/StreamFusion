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

/** Explicit parity decision for fallible TRY_CAST conversions. */
final class StreamFusionTryCastTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionTryCastTranslator() {}

    static String failureReason(Object expression) {
        if ("TRY_CAST".equals(functionName(expression))) {
            return "TRY_CAST stays on Flink unless it is type-preserving or a lossless integer widening; parsing, overflow, rounding, temporal, and nested conversion failures are not yet proven to produce null on exactly the same inputs";
        }
        return null;
    }
}
