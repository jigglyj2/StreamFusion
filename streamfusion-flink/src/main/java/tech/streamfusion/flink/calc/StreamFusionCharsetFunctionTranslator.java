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

/** Explicit parity decisions for JVM character-set conversion functions. */
final class StreamFusionCharsetFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionCharsetFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("ENCODE".equals(function) || "DECODE".equals(function)) {
            return function
                    + " stays on Flink because charset names, installed providers, malformed-input replacement, and unsupported-charset failures are defined by the running JVM and have no parity-proven native equivalent";
        }
        return null;
    }
}
