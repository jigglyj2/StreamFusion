/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
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
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

/** Delegates Flink state while checkpointing a registered native RocksDB participant. */
public final class StreamFusionKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>, InternalCheckpointListener {
    private static final byte[] META_MAGIC = new byte[] {'S', 'F', 'I', '1'};

    private final CheckpointableKeyedStateBackend<K> delegate;
    private final List<IncrementalRemoteKeyedStateHandle> restoredNativeHandles;
    private final UUID backendIdentifier;
    private final String nativeBackendType;
    private final NativeRocksDbMemoryLease nativeRocksDbMemory;
    private final Map<Long, Map<String, SharedFile>> pendingSharedFiles = new ConcurrentHashMap<>();
    private volatile Map<String, SharedFile> completedSharedFiles = Map.of();
    private volatile long completedCheckpointId = -1;
    private volatile NativeIncrementalStateParticipant participant;
    private volatile boolean nativeFileSnapshotsEnabled;
    private final boolean configuredIncrementalCheckpoints;
    private final org.apache.flink.core.fs.CloseableRegistry uploads = new org.apache.flink.core.fs.CloseableRegistry();

    StreamFusionKeyedStateBackend(
            CheckpointableKeyedStateBackend<K> delegate,
            List<IncrementalRemoteKeyedStateHandle> restoredNativeHandles,
            String nativeBackendType,
            NativeRocksDbMemoryLease nativeRocksDbMemory,
            boolean configuredIncrementalCheckpoints) {
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
        for (IncrementalRemoteKeyedStateHandle handle : restoredNativeHandles) {
            Path directory = materialize(handle);
            try {
                KeyGroupRange assignedRange = handle.getKeyGroupRange().getIntersection(getKeyGroupRange());
                if (!KeyGroupRange.EMPTY_KEY_GROUP_RANGE.equals(assignedRange)) {
                    participant.restoreIncrementalCheckpoint(directory, assignedRange);
                }
            } finally {
                deleteDirectory(directory);
            }
        }
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

    long nativeRocksDbMemoryLimit() {
        return nativeRocksDbMemory == null ? 0 : nativeRocksDbMemory.size();
    }

    static boolean isNativeHandle(IncrementalRemoteKeyedStateHandle handle) {
        StreamStateHandle metadata = handle.getMetaDataStateHandle();
        try {
            byte[] bytes;
            if (metadata.asBytesIfInMemory().isPresent()) {
                bytes = metadata.asBytesIfInMemory().get();
            } else {
                try (InputStream input = metadata.openInputStream()) {
                    bytes = input.readNBytes(META_MAGIC.length);
                }
            }
            return bytes.length >= META_MAGIC.length
                    && java.util.Arrays.equals(java.util.Arrays.copyOf(bytes, META_MAGIC.length), META_MAGIC);
        } catch (IOException ignored) {
            return false;
        }
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
        try {
            localCheckpoint = current.prepareIncrementalCheckpoint(checkpointId);
        } catch (Exception | Error failure) {
            current.failIncrementalCheckpoint(checkpointId);
            throw failure;
        }
        var resources = new NativeCheckpointUploadResources(localCheckpoint, streamFactory, () -> {
            pendingSharedFiles.remove(checkpointId);
            current.failIncrementalCheckpoint(checkpointId);
        });
        try {
            return new NativeCheckpointUploadTask(
                    () -> {
                        IncrementalUpload upload =
                                uploadIncrementalCheckpoint(checkpointId, localCheckpoint, resources);
                        resources.onPublication(() -> {
                            current.completeIncrementalCheckpoint(
                                    checkpointId, upload.uploadedBytes, upload.reusedBytes);
                            pendingSharedFiles.put(checkpointId, upload.sharedFiles);
                        });
                        return SnapshotResult.of(upload.handle);
                    },
                    resources,
                    uploads);
        } catch (Exception | Error failure) {
            resources.close();
            throw failure;
        }
    }

    private IncrementalUpload uploadIncrementalCheckpoint(
            long checkpointId, Path directory, NativeCheckpointUploadResources resources) throws Exception {
        List<HandleAndLocalPath> shared = new ArrayList<>();
        List<HandleAndLocalPath> exclusive = new ArrayList<>();
        Map<String, SharedFile> nextSharedFiles = new HashMap<>();
        List<String> emptyFiles = new ArrayList<>();
        long checkpointedSize = 0;
        long reusedBytes = 0;
        List<Path> files;
        try (Stream<Path> paths = Files.walk(directory)) {
            files = paths.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
        for (Path file : files) {
            resources.checkCancelled();
            String relativePath = safeRelativePath(directory, file);
            long size = Files.size(file);
            if (size == 0) {
                emptyFiles.add(relativePath);
                continue;
            }
            // Flink's RocksNativeFullSnapshotStrategy uploads every file privately. Only
            // incremental checkpoints may share immutable SSTs across completed snapshots.
            if (configuredIncrementalCheckpoints && relativePath.endsWith(".sst")) {
                SharedFile previous = completedSharedFiles.get(relativePath);
                StreamStateHandle handle;
                if (previous != null && previous.size == size && resources.canReuse(previous.handle)) {
                    handle = previous.handle;
                    resources.reuse(handle);
                    reusedBytes += size;
                } else {
                    handle = resources.upload(file, CheckpointedStateScope.SHARED);
                    checkpointedSize += size;
                }
                shared.add(HandleAndLocalPath.of(handle, relativePath));
                nextSharedFiles.put(relativePath, new SharedFile(handle, size));
            } else {
                exclusive.add(
                        HandleAndLocalPath.of(resources.upload(file, CheckpointedStateScope.EXCLUSIVE), relativePath));
                checkpointedSize += size;
            }
        }
        byte[] metadataBytes = metadataBytes(emptyFiles);
        StreamStateHandle metadata = resources.upload(metadataBytes, CheckpointedStateScope.EXCLUSIVE);
        checkpointedSize += metadataBytes.length;
        return new IncrementalUpload(
                new IncrementalRemoteKeyedStateHandle(
                        backendIdentifier,
                        getKeyGroupRange(),
                        checkpointId,
                        shared,
                        exclusive,
                        metadata,
                        checkpointedSize),
                checkpointedSize,
                reusedBytes,
                nextSharedFiles);
    }

    private static Path materialize(IncrementalRemoteKeyedStateHandle handle) throws IOException {
        Path directory = Files.createTempDirectory("streamfusion-rocks-restore-");
        try {
            List<HandleAndLocalPath> files = new ArrayList<>(handle.getSharedState());
            files.addAll(handle.getPrivateState());
            for (HandleAndLocalPath file : files) {
                Path target = directory.resolve(file.getLocalPath()).normalize();
                if (!target.startsWith(directory)) {
                    throw new IOException("RocksDB checkpoint path escapes its restore directory");
                }
                Files.createDirectories(target.getParent());
                try (InputStream input = file.getHandle().openInputStream()) {
                    Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            for (String emptyFile : emptyFiles(handle.getMetaDataStateHandle())) {
                Path target = directory.resolve(emptyFile).normalize();
                if (!target.startsWith(directory)) {
                    throw new IOException("RocksDB metadata path escapes its restore directory");
                }
                Files.createDirectories(target.getParent());
                Files.createFile(target);
            }
            return directory;
        } catch (Throwable failure) {
            deleteDirectory(directory);
            throw failure;
        }
    }

    private static byte[] metadataBytes(List<String> emptyFiles) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.write(META_MAGIC);
            output.writeInt(emptyFiles.size());
            for (String path : emptyFiles) {
                byte[] encoded = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                output.writeInt(encoded.length);
                output.write(encoded);
            }
        }
        return bytes.toByteArray();
    }

    private static List<String> emptyFiles(StreamStateHandle metadata) throws IOException {
        byte[] bytes;
        if (metadata.asBytesIfInMemory().isPresent()) {
            bytes = metadata.asBytesIfInMemory().get();
        } else {
            try (InputStream input = metadata.openInputStream()) {
                bytes = input.readAllBytes();
            }
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte[] magic = new byte[META_MAGIC.length];
            input.readFully(magic);
            if (!java.util.Arrays.equals(magic, META_MAGIC)) {
                throw new IOException("Not a StreamFusion incremental RocksDB manifest");
            }
            int count = input.readInt();
            if (count < 0 || count > 10_000) {
                throw new IOException("Invalid empty-file count in native RocksDB manifest");
            }
            List<String> paths = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                int length = input.readInt();
                if (length < 0 || length > 1 << 20) {
                    throw new IOException("Invalid path length in native RocksDB manifest");
                }
                byte[] path = new byte[length];
                input.readFully(path);
                paths.add(new String(path, java.nio.charset.StandardCharsets.UTF_8));
            }
            return paths;
        }
    }

    private static String safeRelativePath(Path directory, Path file) throws IOException {
        Path relative = directory.relativize(file).normalize();
        if (relative.isAbsolute() || relative.startsWith("..")) {
            throw new IOException("RocksDB checkpoint path escapes its checkpoint directory");
        }
        return relative.toString().replace(file.getFileSystem().getSeparator(), "/");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Map<String, SharedFile> completed = pendingSharedFiles.remove(checkpointId);
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

    private static void deleteDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
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
                if (nativeRocksDbMemory != null) nativeRocksDbMemory.close();
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
                if (nativeRocksDbMemory != null) nativeRocksDbMemory.close();
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

    private static final class SharedFile {
        private final StreamStateHandle handle;
        private final long size;

        private SharedFile(StreamStateHandle handle, long size) {
            this.handle = handle;
            this.size = size;
        }
    }

    private static final class IncrementalUpload {
        private final KeyedStateHandle handle;
        private final long uploadedBytes;
        private final long reusedBytes;
        private final Map<String, SharedFile> sharedFiles;

        private IncrementalUpload(
                KeyedStateHandle handle, long uploadedBytes, long reusedBytes, Map<String, SharedFile> sharedFiles) {
            this.handle = handle;
            this.uploadedBytes = uploadedBytes;
            this.reusedBytes = reusedBytes;
            this.sharedFiles = sharedFiles;
        }
    }
}
