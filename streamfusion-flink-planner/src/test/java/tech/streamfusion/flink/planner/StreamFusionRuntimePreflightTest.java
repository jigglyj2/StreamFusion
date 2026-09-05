/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StreamFusionRuntimePreflightTest {
    @Test
    void acceptsACompleteStreamFusionRuntime() {
        assertThat(StreamFusionExecGraphProcessor.runtimePreflightRejection(
                        getClass().getClassLoader()))
                .isNull();
    }

    @Test
    void rejectsBeforeReplacementWhenRuntimePlanClassesAreHidden() {
        ClassLoader incompleteRuntime = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals("tech.streamfusion.proto.plan.v1.NativePlan")) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };

        assertThat(StreamFusionExecGraphProcessor.runtimePreflightRejection(incompleteRuntime))
                .contains("not consistently visible")
                .contains("tech.streamfusion.proto.plan.v1.NativePlan");
    }
}
