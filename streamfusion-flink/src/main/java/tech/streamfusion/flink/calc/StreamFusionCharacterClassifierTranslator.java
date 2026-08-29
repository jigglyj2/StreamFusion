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

/** Parity decisions for Flink's legacy character-classification functions. */
final class StreamFusionCharacterClassifierTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionCharacterClassifierTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("IS_ALPHA".equals(function) || "IS_DIGIT".equals(function)) {
            return function
                    + " stays on Flink because Flink classifies UTF-16 code units with the JVM Character tables; Rust Unicode scalar predicates differ for supplementary characters and can use a different Unicode version";
        }
        if ("IS_DECIMAL".equals(function)) {
            return "IS_DECIMAL stays on Flink because Flink delegates character input to Java integer, long, and double parsers, whose accepted syntax and overflow behavior are not provided by a parity-proven native parser";
        }
        return null;
    }
}
