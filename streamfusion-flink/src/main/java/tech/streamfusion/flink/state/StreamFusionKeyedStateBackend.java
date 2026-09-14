/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractIncrementalStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

/** Delegates Flink state while checkpointing a registered native RocksDB participant. */
public final class StreamFusionKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>, InternalCheckpointListener {

    private final CheckpointableKeyedStateBackend<K> delegate;
    private final List<AbstractIncrementalStateHandle> restoredNativeHandles;
    private final org.apache.flink.runtime.state.LocalRecoveryConfig localRecovery;
    private final UUID backendIdentifier;
    private final String nativeBackendType;
    private final NativeRocksDbMemoryLease nativeRocksDbMemory;
    private final Path nativeRocksDbStorageRoot;
    private final NativeRocksDbTransfers nativeRocksDbTransfers;
    private final List<Integer> nativeRocksDbStatistics;
    private final tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions;
    private final Map<Long, Map<String, NativeCheckpointUpload.SharedFile>> pendingSharedFiles =
            new ConcurrentHashMap<>();
    private volatile Map<String, NativeCheckpointUpload.SharedFile> completedSharedFiles = Map.of();
    private volatile long completedCheckpointId = -1;
    private volatile NativeIncrementalStateParticipant participant;
    private NativeCheckpointRestore preparedRestore;
    private java.io.Closeable preparedRegionOwner;
    private volatile boolean nativeFileSnapshotsEnabled;
    private final boolean configuredIncrementalCheckpoints;
    private final org.apache.flink.core.fs.CloseableRegistry uploads = new org.apache.flink.core.fs.CloseableRegistry();

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints) {
        this(
                delegate,
                restoredNativeHandles,
                nativeBackendType,
                nativeRocksDbMemory,
                configuredIncrementalCheckpoints,
                null);
    }

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints,
            tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions) {
        this(
                delegate,
                restoredNativeHandles,
                nativeBackendType,
                nativeRocksDbMemory,
                configuredIncrementalCheckpoints,
                nativeRocksDbOptions,
                null);
    }

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints,
            tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions,
            Path nativeRocksDbStorageRoot) {
        this(
                delegate,
                restoredNativeHandles,
                nativeBackendType,
                nativeRocksDbMemory,
                configuredIncrementalCheckpoints,
                nativeRocksDbOptions,
                nativeRocksDbStorageRoot,
                null);
    }

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints,
            tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions,
            Path nativeRocksDbStorageRoot,
            NativeRocksDbTransfers nativeRocksDbTransfers) {
        this(
                delegate,
                restoredNativeHandles,
                nativeBackendType,
                nativeRocksDbMemory,
                configuredIncrementalCheckpoints,
                nativeRocksDbOptions,
                nativeRocksDbStorageRoot,
                nativeRocksDbTransfers,
                List.of());
    }

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints,
            tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions,
            Path nativeRocksDbStorageRoot,
            NativeRocksDbTransfers nativeRocksDbTransfers,
            List<Integer> nativeRocksDbStatistics) {
        this(
                delegate,
                restoredNativeHandles,
                nativeBackendType,
                nativeRocksDbMemory,
                configuredIncrementalCheckpoints,
                nativeRocksDbOptions,
                nativeRocksDbStorageRoot,
                nativeRocksDbTransfers,
                nativeRocksDbStatistics,
                org.apache.flink.runtime.state.LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED);
    }

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<? extends AbstractIncrementalStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints,
            tech.streamfusion.proto.plan.v1.NativeRocksDbOptions nativeRocksDbOptions,
            Path nativeRocksDbStorageRoot,
            NativeRocksDbTransfers nativeRocksDbTransfers,
            List<Integer> nativeRocksDbStatistics,
            org.apache.flink.runtime.state.LocalRecoveryConfig localRecovery) {
        this.localRecovery = localRecovery;
        this.nativeRocksDbStatistics = List.copyOf(nativeRocksDbStatistics);
        this.nativeRocksDbTransfers = nativeRocksDbTransfers;
        this.nativeRocksDbStorageRoot = nativeRocksDbStorageRoot;
        this.nativeRocksDbOptions = nativeRocksDbOptions;
        this.delegate = delegate;
        this.configuredIncrementalCheckpoints = configuredIncrementalCheckpoints;
        this.restoredNativeHandles = new ArrayList<>(restoredNativeHandles);
        this.nativeBackendType = nativeBackendType;
        this.nativeRocksDbMemory = nativeRocksDbMemory;
        this.backendIdentifier = restoredNativeHandles.isEmpty()
                ? UUID.randomUUID()
                : restoredNativeHandles.get(0).getBackendIdentifier();
    }

    public void registerNativeStateParticipant(
            NativeIncrementalStateParticipant participant, boolean nativeFileSnapshotsEnabled) throws Exception {
        if (this.participant != null) {
            throw new IllegalStateException("A native state participant is already registered");
        }
        this.participant = participant;
        this.nativeFileSnapshotsEnabled = nativeFileSnapshotsEnabled;
        if (preparedRestore == null && !restoredNativeHandles.isEmpty()) {
            throw new IllegalStateException("Native checkpoint files must be prepared during keyed-backend creation");
        }
        if (preparedRestore != null) preparedRestore.restore(participant);
    }

    void ownPreparedNativeRegion(AutoCloseable owner) throws IOException {
        if (preparedRegionOwner != null) throw new IllegalStateException("Prepared native region already owned");
        preparedRegionOwner = () -> {
            try {
                owner.close();
            } catch (IOException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IOException("Could not close prepared native region", failure);
            }
        };
        uploads.registerCloseable(preparedRegionOwner);
    }

    void claimPreparedNativeRegion() throws IOException {
        if (preparedRegionOwner == null || !uploads.unregisterCloseable(preparedRegionOwner)) {
            throw new IOException("Prepared native region closed before operator initialization");
        }
        preparedRegionOwner = null;
    }

    void prepareNativeRestore(org.apache.flink.core.fs.CloseableRegistry cancellation) throws Exception {
        if (preparedRestore != null) throw new IllegalStateException("Native restore already prepared");
        preparedRestore = NativeCheckpointRestore.prepare(
                restoredNativeHandles,
                getKeyGroupRange(),
                nativeRocksDbStorageRoot,
                nativeRocksDbTransfers,
                cancellation);
        uploads.registerCloseable(preparedRestore);
        restoredNativeHandles.clear();
    }

    /** Both full and incremental RocksDB checkpoints use native files, as in Flink. */
    public boolean usesNativeFileCheckpoints() {
        return participant != null && nativeFileSnapshotsEnabled;
    }

    public boolean usesNativeIncrementalCheckpoints() {
        return usesNativeFileCheckpoints() && configuredIncrementalCheckpoints;
    }

    java.util.UUID nativeRocksDbMemoryScope() {
        return nativeRocksDbMemory == null ? null : nativeRocksDbMemory.scopeId();
    }

    NativeRocksDbMemoryConfiguration nativeRocksDbMemoryConfiguration() {
        return nativeRocksDbMemory == null
                ? NativeRocksDbMemoryConfiguration.DEFAULT
                : nativeRocksDbMemory.configuration();
    }

    tech.streamfusion.proto.plan.v1.NativeStateBinding bindNativeRocksDbConfiguration(
            tech.streamfusion.proto.plan.v1.NativeStateBinding binding) {
        binding = NativeRocksDbConfiguration.bind(
                nativeRocksDbMemoryConfiguration().bind(binding), nativeRocksDbOptions);
        if (!binding.hasRocksdb()) return binding;
        return binding.toBuilder()
                .setRocksdb(binding.getRocksdb().toBuilder()
                        .clearStatisticsTickers()
                        .addAllStatisticsTickers(nativeRocksDbStatistics))
                .build();
    }

    java.nio.file.Path nativeRocksDbDefaultLogDirectory(java.nio.file.Path database) {
        return NativeRocksDbLogDirectory.resolve(database, nativeRocksDbOptions);
    }

    Path nativeRocksDbStorageRoot() {
        return nativeRocksDbStorageRoot;
    }

    long nativeRocksDbMemoryLimit() {
        return nativeRocksDbMemory == null ? 0 : nativeRocksDbMemory.size();
    }

    static boolean isNativeHandle(IncrementalRemoteKeyedStateHandle handle) {
        return NativeCheckpointMetadata.isNativeHandle(handle);
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions)
            throws Exception {
        NativeIncrementalStateParticipant current = participant;
        if (current == null
                || !nativeFileSnapshotsEnabled
                || checkpointOptions.getCheckpointType().isSavepoint()) {
            return delegate.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        }
        Path localCheckpoint;
        Path backupDirectory = null;
        try {
            backupDirectory = NativeCheckpointLocalBackup.prepare(localRecovery, backendIdentifier, checkpointId);
            localCheckpoint = backupDirectory == null
                    ? current.prepareIncrementalCheckpoint(checkpointId)
                    : current.prepareIncrementalCheckpoint(checkpointId, backupDirectory);
        } catch (Exception | Error failure) {
            if (backupDirectory != null) {
                try {
                    org.apache.flink.util.FileUtils.deleteDirectory(backupDirectory.toFile());
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            current.failIncrementalCheckpoint(checkpointId);
            throw failure;
        }
        var resources = new NativeCheckpointUploadResources(
                localCheckpoint,
                streamFactory,
                () -> {
                    pendingSharedFiles.remove(checkpointId);
                    current.failIncrementalCheckpoint(checkpointId);
                },
                localRecovery,
                checkpointId);
        try {
            return new NativeCheckpointUploadTask(
                    () -> {
                        NativeCheckpointUpload.Result upload = NativeCheckpointUpload.upload(
                                backendIdentifier,
                                getKeyGroupRange(),
                                checkpointId,
                                configuredIncrementalCheckpoints,
                                completedSharedFiles,
                                localCheckpoint,
                                resources,
                                nativeRocksDbTransfers);
                        resources.onPublication(() -> {
                            current.completeIncrementalCheckpoint(
                                    checkpointId, upload.uploadedBytes, upload.reusedBytes);
                            pendingSharedFiles.put(checkpointId, upload.sharedFiles);
                        });
                        return upload.snapshotResult();
                    },
                    resources,
                    uploads,
                    nativeRocksDbTransfers);
        } catch (Exception | Error failure) {
            resources.close();
            throw failure;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Map<String, NativeCheckpointUpload.SharedFile> completed = pendingSharedFiles.remove(checkpointId);
        if (completed != null && checkpointId > completedCheckpointId) {
            completedCheckpointId = checkpointId;
            completedSharedFiles = Map.copyOf(completed);
        }
        pendingSharedFiles.keySet().removeIf(id -> id < checkpointId);
        if (delegate instanceof org.apache.flink.api.common.state.CheckpointListener) {
            ((org.apache.flink.api.common.state.CheckpointListener) delegate).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        pendingSharedFiles.remove(checkpointId);
        if (delegate instanceof org.apache.flink.api.common.state.CheckpointListener) {
            ((org.apache.flink.api.common.state.CheckpointListener) delegate).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        pendingSharedFiles.keySet().removeIf(id -> id <= checkpointId);
        if (delegate instanceof InternalCheckpointListener) {
            ((InternalCheckpointListener) delegate).notifyCheckpointSubsumed(checkpointId);
        }
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return delegate.getKeyGroupRange();
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        return delegate.savepoint();
    }

    @Override
    public void setCurrentKey(K newKey) {
        delegate.setCurrentKey(newKey);
    }

    @Override
    public K getCurrentKey() {
        return delegate.getCurrentKey();
    }

    @Override
    public void setCurrentKeyAndKeyGroup(K newKey, int keyGroupIndex) {
        delegate.setCurrentKeyAndKeyGroup(newKey, keyGroupIndex);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegate.getKeySerializer();
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {
        delegate.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return delegate.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<K> getKeys(List<String> states, N namespace) {
        return delegate.getKeys(states, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return delegate.getKeysAndNamespaces(state);
    }

    @Override
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor) throws Exception {
        return delegate.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
    }

    @Override
    public <N, S extends State> S getPartitionedState(
            N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return delegate.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
    }

    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDescriptor,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return delegate.createOrUpdateInternalState(namespaceSerializer, stateDescriptor, snapshotTransformFactory);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        return delegate.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public void dispose() {
        try {
            try {
                uploads.close();
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        } finally {
            try {
                delegate.dispose();
            } finally {
                try {
                    if (nativeRocksDbTransfers != null) nativeRocksDbTransfers.close();
                } catch (IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                } finally {
                    if (nativeRocksDbMemory != null) nativeRocksDbMemory.close();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            uploads.close();
        } finally {
            try {
                delegate.close();
            } finally {
                try {
                    if (nativeRocksDbTransfers != null) nativeRocksDbTransfers.close();
                } finally {
                    if (nativeRocksDbMemory != null) nativeRocksDbMemory.close();
                }
            }
        }
    }

    @Override
    public void registerKeySelectionListener(KeyedStateBackend.KeySelectionListener<K> listener) {
        delegate.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeyedStateBackend.KeySelectionListener<K> listener) {
        return delegate.deregisterKeySelectionListener(listener);
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return delegate.isSafeToReuseKVState();
    }

    @Override
    public String getBackendTypeIdentifier() {
        return nativeBackendType;
    }
}
