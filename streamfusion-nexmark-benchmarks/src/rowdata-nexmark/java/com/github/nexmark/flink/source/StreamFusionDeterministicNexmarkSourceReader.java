/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.nexmark.flink.source;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import java.lang.reflect.Field;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.flink.api.connector.source.SourceReaderContext;

/** Makes the upstream generator reproducible and replayable for byte-parity benchmarks. */
final class StreamFusionDeterministicNexmarkSourceReader extends NexmarkSourceReader {
    private static final long RANDOM_SEED = 0x5f37_59df_21ab_4c89L;
    private static final Field GENERATOR_FIELD = field(NexmarkSourceReader.class, "generator");
    private static final Field RANDOM_FIELD = field(NexmarkGenerator.class, "random");

    private final GeneratorConfig config;

    StreamFusionDeterministicNexmarkSourceReader(SourceReaderContext context, GeneratorConfig config) {
        super(context, config, new StreamFusionDeterministicRowDataEventDeserializer());
        this.config = config;
    }

    @Override
    public void addSplits(List<NexmarkSource.NexmarkSourceSplit> splits) {
        super.addSplits(splits);
        NexmarkSource.NexmarkSourceSplit split = splits.get(0);
        GeneratorConfig splitConfig = split.getGeneratorConfig().reconfigure(config, config.isSourceIgnoreStop());
        NexmarkGenerator generator = new NexmarkGenerator(splitConfig, 0, split.getWallClockBaseTime());
        setRandom(generator, RANDOM_SEED ^ splitConfig.getStartEventId());
        for (long emitted = 0; emitted < split.getNumEmittedSoFar(); emitted++) {
            if (!generator.hasNext()) {
                throw new IllegalStateException("Nexmark split restore position exceeds its event count");
            }
            generator.next();
        }
        try {
            GENERATOR_FIELD.set(this, generator);
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Could not install deterministic Nexmark generator", failure);
        }
    }

    private static void setRandom(NexmarkGenerator generator, long seed) {
        try {
            RANDOM_FIELD.set(generator, new SplittableRandom(seed));
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Could not seed Nexmark generator", failure);
        }
    }

    private static Field field(Class<?> owner, String name) {
        try {
            Field field = owner.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }
}
