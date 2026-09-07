/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import org.apache.flink.configuration.ReadableConfig;

/** Common state-resource admission; capability-driven, independent of SQL operator families. */
final class StreamFusionStateMetricAdmission {
    private StreamFusionStateMetricAdmission() {}

    static String unsupportedReason(ReadableConfig config, ClassLoader loader) {
        try {
            return (String) Class.forName("tech.streamfusion.flink.metrics.NativeStateMetricSupport", true, loader)
                    .getMethod("unsupportedReason", ReadableConfig.class)
                    .invoke(null, config);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Native state metric capability inspection failed", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not inspect shared native state metric capabilities", failure);
        }
    }
}
