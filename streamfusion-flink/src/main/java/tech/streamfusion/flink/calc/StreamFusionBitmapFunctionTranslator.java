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

/** Explicit parity decisions for Flink's specialized bitmap logical type. */
final class StreamFusionBitmapFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionBitmapFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        return function != null && function.startsWith("BITMAP_")
                ? function
                        + " stays on Flink until its BITMAP logical type, Java serialization bytes, signed integer domain, null behavior, and native-library round trips are parity-proven"
                : null;
    }
}
