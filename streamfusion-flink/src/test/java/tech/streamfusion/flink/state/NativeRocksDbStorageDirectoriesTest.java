/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

class NativeRocksDbStorageDirectoriesTest {
    @TempDir
    Path temporary;

    @Test
    void generatedConfigurationUsesFlinksParserWithoutProbingWorkerDirectories() throws Exception {
        var a = temporary.resolve("a");
        var b = temporary.resolve("b");
        for (String value : List.of(a.toString(), a + "," + b, a + java.io.File.pathSeparator + b, a + ",")) {
            var config = config(value);
            var flink = new EmbeddedRocksDBStateBackend(true)
                    .configure(config, getClass().getClassLoader());
            assertThat(flink.getDbStoragePaths()).isNotEmpty();
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
            assertThat(a).doesNotExist();
            assertThat(b).doesNotExist();
        }
        for (String value : List.of("", ",", "relative", "s3://bucket/path")) {
            var config = config(value);
            assertThatThrownBy(() -> new EmbeddedRocksDBStateBackend(true)
                            .configure(config, getClass().getClassLoader()))
                    .isInstanceOf(org.apache.flink.configuration.IllegalConfigurationException.class);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .contains(RocksDBOptions.LOCAL_DIRECTORIES.key());
        }
    }

    @Test
    void actualFlinkAndNativeSelectorsSkipUnusableRootsAndCycleThroughTheSameUsableSet() throws Exception {
        var a = temporary.resolve("a");
        var b = temporary.resolve("b");
        var unusable = Files.writeString(temporary.resolve("file"), "user data");
        var flink = new EmbeddedRocksDBStateBackend(true);
        flink.setDbStoragePaths(a.toString(), unusable.toString(), b.toString());
        var nativeDirectories = NativeRocksDbStorageDirectories.fromBackend(flink);
        var expected = new ArrayList<Path>();
        var actual = new ArrayList<Path>();
        try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(32L << 20)
                        .build();
                var cancel = new CloseableRegistry()) {
            for (int i = 0; i < 6; i++) {
                var backend = flink.createKeyedStateBackend(parameters(environment, "operator", cancel));
                var field = backend.getClass().getDeclaredField("instanceBasePath");
                field.setAccessible(true);
                var directory = ((java.io.File) field.get(backend)).toPath();
                expected.add(directory.getParent());
                actual.add(nativeDirectories.next(environment));
                backend.close();
                backend.dispose();
                assertThat(directory).doesNotExist();
            }
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
        for (var selected : List.of(expected, actual)) {
            assertThat(selected).containsOnly(a, b);
            for (int i = 1; i < selected.size(); i++)
                assertThat(selected.get(i)).isNotEqualTo(selected.get(i - 1));
            assertThat(selected.stream().filter(a::equals).count()).isEqualTo(3);
        }
        assertThat(Files.readString(unusable)).isEqualTo("user data");
        try (var entries = Files.list(a)) {
            assertThat(entries.count()).isZero();
        }
        try (var entries = Files.list(b)) {
            assertThat(entries.count()).isZero();
        }
    }

    @Test
    void defaultRootIsTheTaskManagerWorkingDirectoryAndExhaustedRootsCanBeRetried() throws Exception {
        var working = temporary.resolve("task-working");
        try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(32L << 20)
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(new Configuration(), working.toFile()))
                        .build();
                var cancel = new CloseableRegistry()) {
            var flink = new EmbeddedRocksDBStateBackend(true);
            assertThat(NativeRocksDbStorageDirectories.fromBackend(flink).next(environment))
                    .isEqualTo(working);
            var actual = flink.createKeyedStateBackend(parameters(environment, "operator", cancel));
            var field = actual.getClass().getDeclaredField("instanceBasePath");
            field.setAccessible(true);
            assertThat(((java.io.File) field.get(actual)).toPath().getParent()).isEqualTo(working);
            actual.close();
            actual.dispose();

            var root = Files.writeString(temporary.resolve("unusable"), "test");
            flink = new EmbeddedRocksDBStateBackend(true);
            flink.setDbStoragePath(root.toString());
            var nativeDirectories = NativeRocksDbStorageDirectories.fromBackend(flink);
            var failedFlink = flink;
            assertThatThrownBy(() -> failedFlink.createKeyedStateBackend(parameters(environment, "operator", cancel)))
                    .hasMessageContaining("No local storage directories available");
            assertThatThrownBy(() -> nativeDirectories.next(environment))
                    .hasMessageContaining("No local storage directories available")
                    .hasMessageContaining(root.toString());
            Files.delete(root);
            assertThat(nativeDirectories.next(environment)).isEqualTo(root);
            assertThat(root).isDirectory();
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @Test
    void programmaticFileUriPathsSurviveSerializationAndAreBoundWithoutCreatingJavaRocksDb() throws Exception {
        var directory = temporary.resolve("native");
        var delegate = new NativeStateOwnershipTest.ObservingRocksDbBackend();
        delegate.setDbStoragePath(directory.toUri().toString());
        var wrapper = org.apache.flink.util.InstantiationUtil.clone(new StreamFusionStateBackend(delegate));
        try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(16L << 20)
                        .build();
                var cancel = new CloseableRegistry()) {
            var stream = new StreamConfig(new Configuration());
            stream.setOperatorID(new OperatorID());
            NativeStateOwnership.register(environment, stream, StreamFusionArrowNativeRegionOperator.class);
            String identifier = new OperatorSubtaskDescriptionText(
                            stream.getOperatorID(), StreamFusionArrowNativeRegionOperator.class.getSimpleName(), 0, 1)
                    .toString();
            var backend = (StreamFusionKeyedStateBackend<Integer>)
                    wrapper.createKeyedStateBackend(parameters(environment, identifier, cancel));
            assertThat(backend.nativeRocksDbStorageRoot()).isEqualTo(directory);
            assertThat(backend.nativeRocksDbMemoryLimit()).isPositive();
            backend.close();
            backend.dispose();
            assertThat(directory).isDirectory();
            try (var entries = Files.list(directory)) {
                assertThat(entries.count()).isZero();
            }
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
            Files.delete(directory);
            Files.writeString(directory, "unusable root");
            assertThatThrownBy(() -> new StreamFusionStateBackend(delegate)
                            .createKeyedStateBackend(parameters(environment, identifier, cancel)))
                    .hasMessageContaining("No local storage directories available");
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
            assertThat(Files.readString(directory)).isEqualTo("unusable root");
        }
    }

    private static Configuration config(String paths) {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBOptions.LOCAL_DIRECTORIES, paths);
        return config;
    }

    private static KeyedStateBackendParametersImpl<Integer> parameters(
            MockEnvironment environment, String identifier, CloseableRegistry cancel) {
        return new KeyedStateBackendParametersImpl<>(
                environment,
                environment.getJobID(),
                identifier,
                IntSerializer.INSTANCE,
                1,
                new KeyGroupRange(0, 0),
                environment.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                environment.getMetricGroup(),
                (name, value) -> {},
                List.of(),
                cancel,
                0.5);
    }
}
