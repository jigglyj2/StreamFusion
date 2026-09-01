/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Shared Flink lifecycle for persistent native keyed operators. */
public abstract class AbstractStreamFusionArrowKeyedStateOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements BoundedOneInput, NativeIncrementalStateParticipant {
    private final byte[] serializedPlan;
    private final String stateName;

    private transient long nativeHandle;
    private transient KeyGroupRange keyGroupRange;
    private transient FlinkManagedMemory managedMemory;
    private transient BufferAllocator allocator;
    private transient Path rocksDbDirectory;
    private transient boolean writeRawKeyedSnapshot = true;

    protected AbstractStreamFusionArrowKeyedStateOperator(byte[] serializedPlan, String stateName) {
        this.serializedPlan = serializedPlan.clone();
        this.stateName = stateName;
    }

    @Override
    public final void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (!(getKeyedStateBackend() instanceof CheckpointableKeyedStateBackend)) {
            throw new IllegalStateException("Native " + stateName + " requires a checkpointable keyed state backend");
        }
        keyGroupRange = ((CheckpointableKeyedStateBackend<?>) getKeyedStateBackend()).getKeyGroupRange();
        int maxParallelism = getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-" + stateName.replace(' ', '-'));
        allocator = managedMemory.allocator();
        String backendType = getKeyedStateBackend().getBackendTypeIdentifier();
        if ("rocksdb".equals(backendType)) {
            Path spillDirectory = getContainingTask()
                    .getEnvironment()
                    .getIOManager()
                    .getSpillingDirectories()[0]
                    .toPath();
            rocksDbDirectory = Files.createTempDirectory(spillDirectory, "streamfusion-rocksdb-");
            long rocksDbMemory = managedMemory.limit() * 3 / 4;
            if (!managedMemory.tryReserve(rocksDbMemory)) {
                throw new IllegalStateException("Flink denied " + rocksDbMemory + " bytes for native RocksDB state");
            }
            try {
                nativeHandle = createRocksDbHandle(
                        serializedPlan,
                        maxParallelism,
                        keyGroupRange.getStartKeyGroup(),
                        keyGroupRange.getEndKeyGroup(),
                        rocksDbDirectory,
                        rocksDbMemory,
                        managedMemory);
            } catch (RuntimeException failure) {
                managedMemory.release(rocksDbMemory);
                throw failure;
            }
        } else if ("hashmap".equals(backendType)) {
            nativeHandle = createMemoryHandle(
                    serializedPlan,
                    maxParallelism,
                    keyGroupRange.getStartKeyGroup(),
                    keyGroupRange.getEndKeyGroup(),
                    managedMemory);
        } else {
            throw new IllegalStateException(
                    "Native " + stateName + " supports Flink hashmap and RocksDB state backends, got " + backendType);
        }
        if (getKeyedStateBackend() instanceof StreamFusionKeyedStateBackend) {
            ((StreamFusionKeyedStateBackend<?>) getKeyedStateBackend())
                    .registerNativeStateParticipant(this, "rocksdb".equals(backendType));
        }
        for (KeyGroupStatePartitionStreamProvider provider : context.getRawKeyedStateInputs()) {
            DataInputStream input = new DataInputStream(provider.getStream());
            int length = input.readInt();
            if (length < 0) {
                throw new IOException(
                        "Negative native " + stateName + " state length for key group " + provider.getKeyGroupId());
            }
            byte[] state = new byte[length];
            input.readFully(state);
            restoreKeyGroup(nativeHandle, provider.getKeyGroupId(), state);
        }
    }

    @Override
    protected final boolean isUsingCustomRawKeyedState() {
        return true;
    }

    protected final long nativeHandle() {
        return nativeHandle;
    }

    protected final BufferAllocator allocator() {
        return allocator;
    }

    protected final NativeMemoryManager memoryManager() {
        return managedMemory;
    }

    protected final List<byte[]> preencodeKeys(
            ArrowRowDataBatch input, RowDataKeySelector keySelector, String operatorName) throws Exception {
        List<byte[]> keys = new ArrayList<>(input.size());
        for (int row = 0; row < input.size(); row++) {
            RowData selected = keySelector.getKey(input.rowView(row));
            if (!(selected instanceof BinaryRowData)) {
                throw new IllegalStateException(
                        "Native " + operatorName + " requires Flink's BinaryRowData key selector");
            }
            BinaryRowData binary = (BinaryRowData) selected;
            keys.add(BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        return keys;
    }

    protected static boolean requiresPreencodedKeys(RowType rowType, int[] keyFields) {
        for (int key : keyFields) {
            switch (rowType.getTypeAt(key).getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                    break;
                default:
                    return true;
            }
        }
        return false;
    }

    @Override
    public final void endInput() {}

    @Override
    public final OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        boolean incremental = getKeyedStateBackend() instanceof StreamFusionKeyedStateBackend
                && ((StreamFusionKeyedStateBackend<?>) getKeyedStateBackend()).usesNativeIncrementalCheckpoints()
                && !checkpointOptions.getCheckpointType().isSavepoint();
        writeRawKeyedSnapshot = !incremental;
        try {
            return super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
        } finally {
            writeRawKeyedSnapshot = true;
        }
    }

    @Override
    public final void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (!writeRawKeyedSnapshot) {
            return;
        }
        KeyedStateCheckpointOutputStream output = context.getRawKeyedOperatorStateOutput();
        DataOutputStream framedOutput = new DataOutputStream(output);
        for (int keyGroup : keyGroupRange) {
            output.startNewKeyGroup(keyGroup);
            byte[] state = snapshotKeyGroup(nativeHandle, keyGroup);
            framedOutput.writeInt(state.length);
            framedOutput.write(state);
        }
    }

    @Override
    public final Path prepareIncrementalCheckpoint(long checkpointId) {
        if (rocksDbDirectory == null) {
            throw new IllegalStateException("Only native RocksDB state supports incremental checkpoints");
        }
        Path checkpointDirectory = rocksDbDirectory.resolveSibling(
                "streamfusion-rocks-checkpoint-" + checkpointId + "-" + java.util.UUID.randomUUID());
        checkpointRocks(nativeHandle, checkpointDirectory);
        return checkpointDirectory;
    }

    @Override
    public final void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange restoredRange) {
        long restoreReaderMemory = 256L * 1024;
        if (!managedMemory.tryReserve(restoreReaderMemory)) {
            throw new IllegalStateException(
                    "Flink denied " + restoreReaderMemory + " bytes for the native RocksDB restore reader");
        }
        try {
            importRocksCheckpoint(
                    nativeHandle,
                    checkpointDirectory,
                    restoredRange.getStartKeyGroup(),
                    restoredRange.getEndKeyGroup(),
                    restoreReaderMemory);
        } finally {
            managedMemory.release(restoreReaderMemory);
        }
    }

    @Override
    public final void close() throws Exception {
        long handle = nativeHandle;
        nativeHandle = 0;
        try {
            if (handle != 0) {
                destroyHandle(handle);
            }
        } finally {
            try {
                if (managedMemory != null) {
                    managedMemory.close();
                    managedMemory = null;
                    allocator = null;
                }
            } finally {
                try {
                    deleteDirectory(rocksDbDirectory);
                    rocksDbDirectory = null;
                } finally {
                    super.close();
                }
            }
        }
    }

    protected abstract long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager);

    protected abstract long createRocksDbHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager);

    protected abstract byte[] snapshotKeyGroup(long handle, int keyGroup);

    protected abstract void restoreKeyGroup(long handle, int keyGroup, byte[] state);

    protected abstract void checkpointRocks(long handle, Path checkpointDirectory);

    protected abstract void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit);

    protected abstract void destroyHandle(long handle);

    private static void deleteDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new)) {
                Files.deleteIfExists(path);
            }
        }
    }
}
