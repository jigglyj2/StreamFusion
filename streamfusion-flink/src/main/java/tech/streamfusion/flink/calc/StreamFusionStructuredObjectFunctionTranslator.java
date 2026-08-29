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

/** Explicit parity decisions for class-backed Flink structured objects. */
final class StreamFusionStructuredObjectFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionStructuredObjectFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("OBJECT_OF".equals(function) || "OBJECT_UPDATE".equals(function)) {
            return function
                    + " stays on Flink because class-backed structured types preserve classloader identity, class name, field order, constructors, and object materialization semantics that an Arrow struct does not represent";
        }
        return null;
    }
}
