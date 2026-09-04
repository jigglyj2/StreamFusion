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
    private static final String NATIVE_OPERATOR_PREFIX = "streamfusion-";

    private final StateBackend delegate;
    private final String nativeBackendType;

    public StreamFusionStateBackend(StateBackend delegate) {
        this.delegate = delegate;
        this.nativeBackendType = backendType(delegate);
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
            throws Exception {
        List<IncrementalRemoteKeyedStateHandle> nativeHandles = new ArrayList<>();
        Collection<KeyedStateHandle> delegateHandles = new ArrayList<>();
        for (KeyedStateHandle handle : parameters.getStateHandles()) {
            if (handle instanceof IncrementalRemoteKeyedStateHandle
                    && StreamFusionKeyedStateBackend.isNativeHandle((IncrementalRemoteKeyedStateHandle) handle)) {
                nativeHandles.add((IncrementalRemoteKeyedStateHandle) handle);
            } else {
                delegateHandles.add(handle);
            }
        }
        KeyedStateBackendParametersImpl<K> delegateParameters = new KeyedStateBackendParametersImpl<>(parameters);
        delegateParameters.setStateHandles(delegateHandles);
        boolean nativeOperator = isNativeOperator(parameters.getOperatorIdentifier());
        NativeRocksDbMemoryLease rocksDbMemory = null;
        StateBackend keyedDelegate = delegate;
        if (nativeOperator && "rocksdb".equals(nativeBackendType)) {
            // The Java keyed backend is only a Flink lifecycle/key-group shell for a native
            // operator. Giving it another RocksDB instance would double both memory and snapshots.
            keyedDelegate = new HashMapStateBackend();
            rocksDbMemory = NativeRocksDbMemoryLease.reserve(parameters);
        }
        try {
            return new StreamFusionKeyedStateBackend<>(
                    keyedDelegate.createKeyedStateBackend(delegateParameters),
                    nativeHandles,
                    nativeBackendType,
                    rocksDbMemory);
        } catch (Throwable failure) {
            if (rocksDbMemory != null) {
                rocksDbMemory.close();
            }
            throw failure;
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

    static boolean isNativeOperator(String operatorIdentifier) {
        return operatorIdentifier != null && operatorIdentifier.startsWith(NATIVE_OPERATOR_PREFIX);
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
