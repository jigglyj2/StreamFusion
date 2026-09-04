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

/** Explicit parity decisions for expressions backed by Java's regular-expression engine. */
final class StreamFusionRegexFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionRegexFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if (function != null && (function.startsWith("REGEXP") || function.contains("SIMILAR"))) {
            return function
                    + " stays on Flink because Flink uses Java Pattern syntax, matching, capture, and replacement semantics; Rust/DataFusion regex deliberately excludes constructs such as look-around and backreferences and is not byte-parity compatible";
        }
        return null;
    }
}
