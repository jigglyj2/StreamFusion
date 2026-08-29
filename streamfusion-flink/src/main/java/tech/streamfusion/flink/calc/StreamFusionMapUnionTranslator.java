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

/** Reports why Flink 2.3 {@code MAP_UNION} cannot safely be replaced. */
final class StreamFusionMapUnionTranslator extends StreamFusionRexSupport {
    private StreamFusionMapUnionTranslator() {}

    static String failureReason(Object expression) {
        if (!"MAP_UNION".equals(functionName(expression))) {
            return null;
        }
        return "MAP_UNION stays on Flink 2.3 because its MapDataForMapUnion representation permits null keys and is observed by representation-sensitive generated consumers; Arrow maps require non-null keys";
    }
}
