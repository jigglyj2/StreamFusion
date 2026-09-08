/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.memory;

import java.io.Serializable;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.graph.StreamConfig;

/** Original physical operator's capacity geometry; it never reserves or grants native memory. */
public final class FlinkOperatorMemoryShare implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Configuration fractions;

    public FlinkOperatorMemoryShare(
            int operatorWeight, int groupOperatorWeight, Set<ManagedMemoryUseCase> groupUseCases) {
        if (operatorWeight <= 0
                || groupOperatorWeight < operatorWeight
                || !groupUseCases.contains(ManagedMemoryUseCase.OPERATOR))
            throw new IllegalArgumentException(
                    "Original Flink operator memory requires positive consistent slot-group weights");
        fractions = new Configuration();
        var original = new StreamConfig(fractions);
        for (var useCase : groupUseCases) original.setManagedMemoryFractionOperatorOfUseCase(useCase, 0.0);
        original.setManagedMemoryFractionOperatorOfUseCase(
                ManagedMemoryUseCase.OPERATOR,
                ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupOperatorWeight));
    }

    public long memoryBytes(Environment environment, StreamConfig runtime) {
        // Preserve Flink's actual backend flag and job/TaskManager precedence. Replace only
        // the use-case fractions, including categories that fusion may have added or removed.
        var copy = new Configuration(runtime.getConfiguration());
        var allFractions = new Configuration();
        var probe = new StreamConfig(allFractions);
        for (var useCase : ManagedMemoryUseCase.values()) probe.setManagedMemoryFractionOperatorOfUseCase(useCase, 0.0);
        for (String key : allFractions.keySet()) copy.removeKey(key);
        copy.addAll(fractions);
        double fraction = new StreamConfig(copy)
                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        environment.getJobConfiguration(),
                        environment.getTaskManagerInfo().getConfiguration(),
                        environment.getUserCodeClassLoader().asClassLoader());
        return environment.getMemoryManager().computeMemorySize(fraction);
    }
}
