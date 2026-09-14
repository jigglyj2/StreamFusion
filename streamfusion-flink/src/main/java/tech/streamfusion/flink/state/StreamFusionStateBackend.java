/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

/** Delegating Flink backend that adds standard incremental handles for native RocksDB state. */
public final class StreamFusionStateBackend implements StateBackend {
    private static final long serialVersionUID = 1L;

    private final StateBackend delegate;
    private final String nativeBackendType;
    // Retain the initial snapshot in the serialized backend without generated-message serialization.
    // Runtime creation also resolves supported public setters, which can change after wrapping.
    private final byte[] nativeRocksDbOptions;
    private final org.apache.flink.configuration.Configuration nativeRocksDbExplicitOptions;
    private final List<Integer> nativeRocksDbStatistics;
    private final String nativeRocksDbConfigurationFailure;
    private transient NativeRocksDbStorageDirectories nativeRocksDbStorageDirectories;

    /** Wraps a default-configured backend; pass its ReadableConfig below if it was configured explicitly. */
    public StreamFusionStateBackend(StateBackend delegate) {
        this(delegate, null);
    }

    /** The config must be the one used to configure the delegate's database and runtime options. */
    public StreamFusionStateBackend(StateBackend delegate, org.apache.flink.configuration.ReadableConfig config) {
        this.delegate = delegate;
        this.nativeBackendType = backendType(delegate);
        byte[] options = null;
        org.apache.flink.configuration.Configuration explicitOptions = null;
        List<Integer> statistics = List.of();
        String failureReason = null;
        try {
            if ("rocksdb".equals(nativeBackendType) && config != null) {
                // Planner preflight is not the only entry point: callers can directly create
                // this wrapper. Defer rejection until native ownership is established so the
                // same wrapper remains usable by a whole-plan Flink fallback.
                String unsupported = NativeStateConfigurationSupport.rocksDbUnsupportedReason(config);
                if (unsupported == null) {
                    unsupported = NativeStateSupport.unsupportedControlAndMetrics(config);
                }
                if (unsupported != null) throw new UnsupportedOperationException(unsupported);
            }
            if ("rocksdb".equals(nativeBackendType)) {
                explicitOptions = NativeRocksDbConfiguration.explicitOptions(
                        config == null ? new org.apache.flink.configuration.Configuration() : config);
                statistics = tech.streamfusion.flink.metrics.NativeRocksDbStatisticsConfiguration.fromConfig(
                        config == null ? new org.apache.flink.configuration.Configuration() : config);
            }
            options = "rocksdb".equals(nativeBackendType)
                    ? NativeRocksDbConfiguration.fromConfig(
                                    config == null ? new org.apache.flink.configuration.Configuration() : config,
                                    delegate.getClass()
                                            .getMethod("getPredefinedOptions")
                                            .invoke(delegate)
                                            .toString())
                            .toBuilder()
                            .setWriteBatchSize((Long) delegate.getClass()
                                    .getMethod("getWriteBatchSize")
                                    .invoke(delegate))
                            .build()
                            .toByteArray()
                    : null;
        } catch (ReflectiveOperationException | IllegalArgumentException | UnsupportedOperationException failure) {
            // The installed wrapper may also serve a subsequent whole-plan fallback. Keep its
            // Java delegate usable; only a registered native owner needs this configuration.
            failureReason = "Cannot preserve native RocksDB configuration: " + failure.getMessage();
        }
        this.nativeRocksDbOptions = options;
        this.nativeRocksDbExplicitOptions = explicitOptions;
        this.nativeRocksDbStatistics = statistics;
        this.nativeRocksDbConfigurationFailure = failureReason;
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
            throws Exception {
        boolean nativeOperator = NativeStateOwnership.owns(parameters);
        List<org.apache.flink.runtime.state.AbstractIncrementalStateHandle> nativeHandles = new ArrayList<>();
        Collection<KeyedStateHandle> delegateHandles = new ArrayList<>();
        for (KeyedStateHandle handle : parameters.getStateHandles()) {
            if (handle instanceof org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle
                    && (nativeOperator
                            || NativeCheckpointMetadata.isNativeHandle(
                                    (org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle) handle,
                                    parameters.getCancelStreamRegistry()))) {
                if (!NativeRegionRestoreBootstrap.isRegistered(parameters)) {
                    throw new IllegalArgumentException(NativeCheckpointLocalBackup.recoveryUnsupportedReason());
                }
                nativeHandles.add((org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle) handle);
            } else if (handle instanceof IncrementalRemoteKeyedStateHandle
                    && NativeCheckpointMetadata.isNativeHandle(
                            (IncrementalRemoteKeyedStateHandle) handle, parameters.getCancelStreamRegistry())) {
                nativeHandles.add((IncrementalRemoteKeyedStateHandle) handle);
            } else {
                delegateHandles.add(handle);
            }
        }
        if (parameters.getCancelStreamRegistry().isClosed()) {
            throw new java.io.IOException("Native checkpoint preparation cancelled");
        }
        KeyedStateBackendParametersImpl<K> delegateParameters = new KeyedStateBackendParametersImpl<>(parameters);
        delegateParameters.setStateHandles(delegateHandles);
        NativeRocksDbMemoryLease rocksDbMemory = null;
        java.nio.file.Path rocksDbStorageRoot = null;
        NativeRocksDbTransfers rocksDbTransfers = null;
        tech.streamfusion.proto.plan.v1.NativeRocksDbOptions databaseOptions = null;
        var localRecovery = org.apache.flink.runtime.state.LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED;
        StateBackend keyedDelegate = delegate;
        if (nativeOperator && "rocksdb".equals(nativeBackendType)) {
            if (nativeRocksDbConfigurationFailure != null) {
                throw new IllegalArgumentException(nativeRocksDbConfigurationFailure);
            }
            // Read these public setters again at creation, including changes made after wrapper
            // construction. Never silently ignore a programmatic override or call its factory.
            NativeRocksDbBackendSupport.validate(delegate);
            databaseOptions = currentNativeRocksDbOptions();
            localRecovery = parameters.getEnv().getTaskStateManager().createLocalRecoveryConfig();
            if (localRecovery.isLocalRecoveryEnabled() && !NativeRegionRestoreBootstrap.isRegistered(parameters)) {
                throw new IllegalArgumentException(NativeCheckpointLocalBackup.recoveryUnsupportedReason());
            }
            // The Java keyed backend is only a Flink lifecycle/key-group shell for a native
            // operator. Giving it another RocksDB instance would double both memory and snapshots.
            keyedDelegate = new HashMapStateBackend();
            rocksDbStorageRoot = nextNativeRocksDbStorageRoot(parameters.getEnv());
            rocksDbMemory = NativeRocksDbMemoryLease.reserve(
                    parameters.getEnv().getMemoryManager(),
                    parameters.getManagedMemoryFraction(),
                    NativeRocksDbMemoryConfiguration.fromBackend(delegate));
        }
        try {
            if (nativeOperator && "rocksdb".equals(nativeBackendType)) {
                // Flink closes the parameters' restore-only registry when construction ends.
                // The returned keyed backend is registered with Flink's task-lifetime registry
                // and owns this transfer helper through close/dispose.
                rocksDbTransfers = NativeRocksDbTransfers.open(
                        delegate, parameters.getEnv(), new org.apache.flink.core.fs.CloseableRegistry());
            }
            var result = new StreamFusionKeyedStateBackend<>(
                    keyedDelegate.createKeyedStateBackend(delegateParameters),
                    nativeHandles,
                    nativeBackendType,
                    rocksDbMemory,
                    configuredIncrementalCheckpoints(),
                    databaseOptions,
                    rocksDbStorageRoot,
                    rocksDbTransfers,
                    nativeRocksDbStatistics == null ? List.of() : nativeRocksDbStatistics,
                    localRecovery);
            try {
                result.prepareNativeRestore(parameters.getCancelStreamRegistry());
                if (nativeOperator && !nativeHandles.isEmpty())
                    NativeRegionRestoreBootstrap.prepare(result, parameters);
                if (parameters.getCancelStreamRegistry().isClosed())
                    throw new java.io.IOException("Native restore cancelled");
                return result;
            } catch (Throwable failure) {
                try {
                    result.close();
                } catch (Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
                try {
                    result.dispose();
                } catch (Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (Throwable failure) {
            if (rocksDbTransfers != null) {
                try {
                    rocksDbTransfers.close();
                } catch (java.io.IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (rocksDbMemory != null) {
                rocksDbMemory.close();
            }
            throw failure;
        }
    }

    private synchronized java.nio.file.Path nextNativeRocksDbStorageRoot(
            org.apache.flink.runtime.execution.Environment environment) throws Exception {
        if (nativeRocksDbStorageDirectories == null) {
            nativeRocksDbStorageDirectories = NativeRocksDbStorageDirectories.fromBackend(delegate);
        }
        return nativeRocksDbStorageDirectories.next(environment);
    }

    tech.streamfusion.proto.plan.v1.NativeRocksDbOptions currentNativeRocksDbOptions()
            throws ReflectiveOperationException {
        if (!"rocksdb".equals(nativeBackendType)) return null;
        if (nativeRocksDbOptions == null || nativeRocksDbExplicitOptions == null) {
            throw new IllegalArgumentException(
                    "Cannot preserve native RocksDB configuration: serialized backend lacks explicit option provenance; "
                            + "recreate the backend from its Flink configuration");
        }
        // Flink reads these public settings while creating each keyed backend. Preset changes
        // must be applied below explicit configuration, while the write-batch setter wins.
        return NativeRocksDbConfiguration.fromConfig(
                        nativeRocksDbExplicitOptions,
                        delegate.getClass()
                                .getMethod("getPredefinedOptions")
                                .invoke(delegate)
                                .toString())
                .toBuilder()
                .setWriteBatchSize((Long)
                        delegate.getClass().getMethod("getWriteBatchSize").invoke(delegate))
                .build();
    }

    // The optional RocksDB integration remains optional on the runtime classpath. Read its
    // configured public API instead of inferring checkpoint behavior from the backend name.
    boolean configuredIncrementalCheckpoints() {
        if (!"rocksdb".equals(nativeBackendType)) return false;
        try {
            return (Boolean) delegate.getClass()
                    .getMethod("isIncrementalCheckpointsEnabled")
                    .invoke(delegate);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException(
                    "Cannot preserve the configured RocksDB incremental-checkpoint setting", failure);
        }
    }

    @Override
    public <K> AsyncKeyedStateBackend<K> createAsyncKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
            throws Exception {
        return delegate.createAsyncKeyedStateBackend(parameters);
    }

    @Override
    public boolean supportsAsyncKeyedStateBackend() {
        return delegate.supportsAsyncKeyedStateBackend();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters) throws Exception {
        return delegate.createOperatorStateBackend(parameters);
    }

    @Override
    public boolean useManagedMemory() {
        return delegate.useManagedMemory();
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        return delegate.supportsNoClaimRestoreMode();
    }

    @Override
    public boolean supportsSavepointFormat(org.apache.flink.core.execution.SavepointFormatType formatType) {
        return delegate.supportsSavepointFormat(formatType);
    }

    @Override
    public String getName() {
        return "StreamFusion(" + delegate.getName() + ")";
    }

    private static String backendType(StateBackend backend) {
        String name = backend.getName().toLowerCase(java.util.Locale.ROOT);
        if (name.contains("rocks")) {
            return "rocksdb";
        }
        if (name.contains("hash") || name.contains("heap")) {
            return "hashmap";
        }
        return name;
    }
}
