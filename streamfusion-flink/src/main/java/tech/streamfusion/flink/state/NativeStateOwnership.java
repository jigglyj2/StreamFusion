/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

/** Task-local ownership registered by native operators before Flink creates their keyed backend. */
public final class NativeStateOwnership {
    // Runtime metadata only: this is populated during operator construction/setup, not SQL planning.
    private static final ConfigOption<List<String>> OWNERS = ConfigOptions.key(
                    "streamfusion.internal.native-state-owners")
            .stringType()
            .asList()
            .defaultValues();

    private NativeStateOwnership() {}

    public static void register(Environment environment, StreamConfig operatorConfig, Class<?> operatorClass) {
        if (!NativeIncrementalStateParticipant.class.isAssignableFrom(operatorClass)
                && operatorClass != StreamFusionArrowNativeRegionOperator.class) {
            throw new IllegalArgumentException("Only native state owners may register a keyed backend");
        }
        var task = environment.getTaskInfo();
        String identifier = new OperatorSubtaskDescriptionText(
                        operatorConfig.getOperatorID(),
                        operatorClass.getSimpleName(),
                        task.getIndexOfThisSubtask(),
                        task.getNumberOfParallelSubtasks())
                .toString();
        var configuration = environment.getTaskConfiguration();
        synchronized (configuration) {
            var owners = new ArrayList<>(configuration.get(OWNERS));
            if (!owners.contains(identifier)) {
                owners.add(identifier);
                configuration.set(OWNERS, owners);
            }
        }
    }

    static boolean owns(StateBackend.KeyedStateBackendParameters<?> parameters) {
        return parameters.getEnv().getTaskConfiguration().get(OWNERS).contains(parameters.getOperatorIdentifier());
    }
}
