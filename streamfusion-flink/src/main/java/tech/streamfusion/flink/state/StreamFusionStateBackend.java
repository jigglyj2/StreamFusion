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

/** Delegating Flink backend that adds standard incremental handles for native RocksDB state. */
public final class StreamFusionStateBackend implements StateBackend {
    private static final long serialVersionUID = 1L;

    private final StateBackend delegate;

    public StreamFusionStateBackend(StateBackend delegate) {
        this.delegate = delegate;
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
        return new StreamFusionKeyedStateBackend<>(delegate.createKeyedStateBackend(delegateParameters), nativeHandles);
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
}
