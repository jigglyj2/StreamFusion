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
package tech.streamfusion.flink.sql;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Owns one Flink mini-cluster for every SQL parity class in the Maven test run. */
final class SharedMiniClusterExtension implements BeforeAllCallback {
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(SharedMiniClusterExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot()
                .getStore(NAMESPACE)
                .getOrComputeIfAbsent(ClusterResource.class, ignored -> new ClusterResource());
    }

    private static final class ClusterResource implements ExtensionContext.Store.CloseableResource, AutoCloseable {
        private final MiniClusterWithClientResource cluster;
        private final java.nio.file.Path workingDirectory;

        private ClusterResource() {
            try {
                // Explicit RocksDB log names flatten the entire database path. JUnit's
                // nested temporary roots can exceed the filesystem filename limit even
                // before the test adds its own paths. Use the same short Flink working
                // directory for both engines; overlong-name failures have separate tests.
                workingDirectory = java.nio.file.Files.createTempDirectory("sf-sql-");
            } catch (java.io.IOException exception) {
                throw new java.io.UncheckedIOException(exception);
            }
            var config = new org.apache.flink.configuration.Configuration();
            config.set(
                    org.apache.flink.configuration.ClusterOptions.PROCESS_WORKING_DIR_BASE,
                    workingDirectory.toString());
            cluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
                    .setConfiguration(config)
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());
            try {
                cluster.before();
            } catch (Exception exception) {
                try {
                    org.apache.flink.util.FileUtils.deleteDirectory(workingDirectory.toFile());
                } catch (java.io.IOException cleanup) {
                    exception.addSuppressed(cleanup);
                }
                throw new IllegalStateException("Could not start the shared Flink mini-cluster", exception);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                cluster.after();
            } finally {
                org.apache.flink.util.FileUtils.deleteDirectory(workingDirectory.toFile());
            }
        }
    }
}
