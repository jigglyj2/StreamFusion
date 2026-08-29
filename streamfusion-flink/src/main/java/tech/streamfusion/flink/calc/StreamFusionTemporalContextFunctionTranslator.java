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

/** Explicit parity decisions for temporal functions tied to Flink runtime context. */
final class StreamFusionTemporalContextFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionTemporalContextFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if (isClockFunction(function)) {
            return function
                    + " stays on Flink because its value and timezone are bound to Flink's job, row, and session clock lifecycle; native batch evaluation cannot independently sample a parity-equivalent clock";
        }
        if ("DATE_FORMAT".equals(function)
                || "FROM_UNIXTIME".equals(function)
                || "UNIX_TIMESTAMP".equals(function)
                || "TO_DATE".equals(function)
                || "TO_TIMESTAMP".equals(function)
                || "TO_TIMESTAMP_LTZ".equals(function)
                || "CONVERT_TZ".equals(function)) {
            return function
                    + " stays on Flink until Java pattern parsing, locale, session-zone, DST gap/overlap, invalid-input, and precision behavior are byte-parity proven";
        }
        if ("TIMESTAMP_DIFF".equals(function) || "TEMPORAL_OVERLAPS".equals(function) || "AT".equals(function)) {
            return function
                    + " stays on Flink until calendar-unit truncation, overflow, interval, and session-zone semantics are byte-parity proven";
        }
        return null;
    }

    private static boolean isClockFunction(String function) {
        return "CURRENT_DATE".equals(function)
                || "CURRENT_TIME".equals(function)
                || "LOCAL_TIME".equals(function)
                || "CURRENT_TIMESTAMP".equals(function)
                || "NOW".equals(function)
                || "CURRENT_ROW_TIMESTAMP".equals(function)
                || "LOCAL_TIMESTAMP".equals(function);
    }
}
