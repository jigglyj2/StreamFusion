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

/** Reports why Flink 2.3 {@code MAP_FROM_ARRAYS} cannot safely be replaced. */
final class StreamFusionMapFromArraysTranslator extends StreamFusionRexSupport {
    private StreamFusionMapFromArraysTranslator() {}

    static String failureReason(Object expression) {
        if (!"MAP_FROM_ARRAYS".equals(functionName(expression))) {
            return null;
        }
        return "MAP_FROM_ARRAYS stays on Flink 2.3 because generated consumers can cast its MapDataForMapFromArrays result to GenericMapData and fail; native success would violate observable failure parity";
    }
}
