/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowDeduplicateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeArrowDeduplicateResult;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.NativeIncrementalStateParticipant;
import tech.streamfusion.flink.state.StreamFusionKeyedStateBackend;
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Stateful keep-last deduplicate whose input and output remain Arrow-backed. */
final class StreamFusionArrowDeduplicateOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                BoundedOneInput,
                NativeIncrementalStateParticipant {
    private final RowType rowType;
    private final byte[] serializedPlan;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    private transient long nativeHandle;
    private transient KeyGroupRange keyGroupRange;
    private transient FlinkManagedMemory managedMemory;
    private transient BufferAllocator allocator;
    private transient Path rocksDbDirectory;
    private transient boolean writeRawKeyedSnapshot = true;

    StreamFusionArrowDeduplicateOperator(
            RowType rowType, int[] uniqueKeys, int orderIndex, boolean generateInsert, RowDataKeySelector keySelector) {
        this.rowType = rowType;
        this.serializedPlan = createPlan(uniqueKeys, orderIndex, generateInsert);
        this.preencodeKeys = requiresPreencodedKeys(rowType, uniqueKeys);
        this.keySelector = keySelector;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (!(getKeyedStateBackend() instanceof CheckpointableKeyedStateBackend)) {
            throw new IllegalStateException("Native deduplicate requires a checkpointable keyed state backend");
        }
        keyGroupRange = ((CheckpointableKeyedStateBackend<?>) getKeyedStateBackend()).getKeyGroupRange();
        int maxParallelism = getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-deduplicate");
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
                nativeHandle = NativeDeduplicateBridge.createRocksDb(
                        serializedPlan,
                        maxParallelism,
                        keyGroupRange.getStartKeyGroup(),
                        keyGroupRange.getEndKeyGroup(),
                        rocksDbDirectory,
                        rocksDbMemory);
            } catch (RuntimeException failure) {
                managedMemory.release(rocksDbMemory);
                throw failure;
            }
        } else if ("hashmap".equals(backendType)) {
            nativeHandle = NativeDeduplicateBridge.create(
                    serializedPlan,
                    maxParallelism,
                    keyGroupRange.getStartKeyGroup(),
                    keyGroupRange.getEndKeyGroup(),
                    managedMemory);
        } else {
            throw new IllegalStateException(
                    "Native deduplicate supports Flink hashmap and RocksDB state backends, got " + backendType);
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
                        "Negative native deduplicate state length for key group " + provider.getKeyGroupId());
            }
            byte[] state = new byte[length];
            input.readFully(state);
            NativeDeduplicateBridge.restore(nativeHandle, provider.getKeyGroupId(), state);
        }
    }

    @Override
    protected boolean isUsingCustomRawKeyedState() {
        return true;
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        for (int row = 0; row < input.size(); row++) {
            if (input.rowKind(row) != org.apache.flink.types.RowKind.INSERT) {
                throw new IllegalStateException(
                        "Native rowtime keep-last deduplicate requires insert-only input, got " + input.rowKind(row));
            }
        }
        List<byte[]> keys = preencodeKeys ? preencodeKeys(input) : null;
        try (NativeArrowDeduplicateResult result =
                ArrowDeduplicateCDataBridge.executeArrow(nativeHandle, input, keys, rowType, allocator)) {
            ArrowRowDataBatch outputBatch = result.selectEnvelopeFrom(input);
            output.collect(new StreamRecord<>(outputBatch));
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
        }
    }

    private List<byte[]> preencodeKeys(ArrowRowDataBatch input) throws Exception {
        List<byte[]> keys = new ArrayList<>(input.size());
        for (int row = 0; row < input.size(); row++) {
            RowData selected = keySelector.getKey(input.rowView(row));
            if (!(selected instanceof BinaryRowData)) {
                throw new IllegalStateException("Native deduplicate requires Flink's BinaryRowData key selector");
            }
            BinaryRowData binary = (BinaryRowData) selected;
            keys.add(BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        return keys;
    }

    private static byte[] createPlan(int[] uniqueKeys, int orderIndex, boolean generateInsert) {
        Deduplicate.Builder deduplicate = Deduplicate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setOrderIndex(orderIndex)
                .setKeepLast(true)
                .setGenerateInsert(generateInsert);
        for (int key : uniqueKeys) {
            deduplicate.addKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setDeduplicate(deduplicate))
                .build()
                .toByteArray();
    }

    private static boolean requiresPreencodedKeys(RowType rowType, int[] uniqueKeys) {
        for (int key : uniqueKeys) {
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
    public void endInput() {}

    @Override
    public OperatorSnapshotFutures snapshotState(
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
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (!writeRawKeyedSnapshot) {
            return;
        }
        KeyedStateCheckpointOutputStream output = context.getRawKeyedOperatorStateOutput();
        DataOutputStream framedOutput = new DataOutputStream(output);
        for (int keyGroup : keyGroupRange) {
            output.startNewKeyGroup(keyGroup);
            byte[] state = NativeDeduplicateBridge.snapshot(nativeHandle, keyGroup);
            framedOutput.writeInt(state.length);
            framedOutput.write(state);
        }
    }

    @Override
    public Path prepareIncrementalCheckpoint(long checkpointId) {
        if (rocksDbDirectory == null) {
            throw new IllegalStateException("Only native RocksDB state supports incremental checkpoints");
        }
        Path checkpointDirectory = rocksDbDirectory.resolveSibling(
                "streamfusion-rocks-checkpoint-" + checkpointId + "-" + java.util.UUID.randomUUID());
        NativeDeduplicateBridge.checkpointRocks(nativeHandle, checkpointDirectory);
        return checkpointDirectory;
    }

    @Override
    public void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange restoredRange) {
        long restoreReaderMemory = 256L * 1024;
        if (!managedMemory.tryReserve(restoreReaderMemory)) {
            throw new IllegalStateException(
                    "Flink denied " + restoreReaderMemory + " bytes for the native RocksDB restore reader");
        }
        try {
            NativeDeduplicateBridge.importRocksCheckpoint(
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
    public void close() throws Exception {
        long handle = nativeHandle;
        nativeHandle = 0;
        try {
            if (handle != 0) {
                NativeDeduplicateBridge.destroy(handle);
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
