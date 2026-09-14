/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Runs the unchanged Flink 2.3 test-jar contracts used by Arrow channel-state recovery. */
class FlinkRecoveryFilterUpstreamTest {
    @TestFactory
    List<DynamicTest> upstreamFilteringContracts() throws Exception {
        var tests = new ArrayList<DynamicTest>();
        for (String name : List.of("RecordFilterTest", "VirtualChannelRecordFilterFactoryTest")) {
            Class<?> type = Class.forName("org.apache.flink.streaming.runtime.io.recovery." + name);
            var constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            for (var method : type.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(Test.class)) continue;
                method.setAccessible(true);
                tests.add(DynamicTest.dynamicTest(name + "." + method.getName(), () -> {
                    try {
                        method.invoke(constructor.newInstance());
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }));
            }
        }
        assertThat(tests).hasSize(9);
        return tests;
    }
}
