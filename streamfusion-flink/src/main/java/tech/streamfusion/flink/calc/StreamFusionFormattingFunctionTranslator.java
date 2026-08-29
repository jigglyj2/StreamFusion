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

/** Explicit parity decisions for JVM formatting functions. */
final class StreamFusionFormattingFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionFormattingFunctionTranslator() {}

    static String failureReason(Object expression) {
        return "PRINTF".equals(functionName(expression))
                ? "PRINTF stays on Flink because java.util.Formatter defines locale, conversion, rounding, and invalid-format exception behavior that has no parity-proven native equivalent"
                : null;
    }
}
