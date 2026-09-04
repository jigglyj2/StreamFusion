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
import org.apache.flink.core.memory.MemorySegmentFactory;
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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowDeduplicateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeDeduplicateResult;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.state.NativeIncrementalStateParticipant;
import tech.streamfusion.flink.state.StreamFusionKeyedStateBackend;
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Test-only RowData parity adapter for the Arrow-native keep-latest implementation. */
final class StreamFusionDeduplicateOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput, NativeIncrementalStateParticipant {
    private static final int BATCH_SIZE = 1024;

    private final RowType rowType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final boolean preencodeKeys;
    private final boolean inputChangelog;
    private final boolean materializePreviousRows;
    private final List<BufferedRow> rows = new ArrayList<>(BATCH_SIZE);

    private transient long nativeHandle;
    private transient KeyGroupRange keyGroupRange;
    private transient FlinkManagedMemory managedMemory;
    private transient BufferAllocator allocator;
    private transient Path rocksDbDirectory;
    private transient boolean writeRawKeyedSnapshot = true;

    StreamFusionDeduplicateOperator(RowType rowType, int[] uniqueKeys, int orderIndex, boolean generateInsert) {
        this(rowType, uniqueKeys, orderIndex, generateInsert, false, false);
    }

    StreamFusionDeduplicateOperator(
            RowType rowType,
            int[] uniqueKeys,
            int orderIndex,
            boolean generateInsert,
            boolean inputChangelog,
            boolean generateUpdateBefore) {
        this.rowType = rowType;
        this.serializer = new RowDataSerializer(physicalRowType(rowType));
        this.preencodeKeys = requiresPreencodedKeys(rowType, uniqueKeys);
        this.inputChangelog = inputChangelog;
        this.materializePreviousRows = inputChangelog || generateUpdateBefore;
        this.serializedPlan = createPlan(uniqueKeys, orderIndex, generateInsert, inputChangelog, generateUpdateBefore);
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
                        rocksDbMemory,
                        managedMemory);
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
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        if (!inputChangelog && row.getRowKind() != RowKind.INSERT) {
            throw new IllegalStateException(
                    "Native keep-latest deduplicate requires insert-only input, got " + row.getRowKind());
        }
        // Flink may reuse its input row after processElement returns. Keep one compact, owned row
        // instead of deep-copying every variable-width field into separate Java objects.
        byte[] key = preencodeKeys ? currentBinaryKey() : null;
        BinaryRowData owned = serializer.toBinaryRow(row).copy();
        rows.add(new BufferedRow(owned, key, element.hasTimestamp(), element.getTimestamp()));
        if (rows.size() == BATCH_SIZE) {
            flushBatch();
        }
    }

    @Override
    public void endInput() {
        flushBatch();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flushBatch();
    }

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
        flushBatch();
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
    public void processWatermark(Watermark watermark) throws Exception {
        flushBatch();
        super.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        flushBatch();
        super.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        flushBatch();
        super.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        flushBatch();
        super.processRecordAttributes(recordAttributes);
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
                    deleteRocksDbDirectory();
                } finally {
                    super.close();
                }
            }
        }
    }

    private void deleteRocksDbDirectory() throws IOException {
        Path directory = rocksDbDirectory;
        rocksDbDirectory = null;
        deleteDirectory(directory);
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

    private void flushBatch() {
        if (rows.isEmpty()) {
            return;
        }
        List<RowData> values = new ArrayList<>(rows.size());
        rows.forEach(row -> values.add(row.value));
        List<byte[]> keys = null;
        if (preencodeKeys) {
            keys = new ArrayList<>(rows.size());
            for (BufferedRow row : rows) {
                keys.add(row.key);
            }
        }
        List<byte[]> storedRows = null;
        List<RowKind> inputKinds = null;
        if (materializePreviousRows) {
            storedRows = new ArrayList<>(rows.size());
            for (BufferedRow row : rows) {
                storedRows.add(BinarySegmentUtils.copyToBytes(
                        row.value.getSegments(), row.value.getOffset(), row.value.getSizeInBytes()));
            }
        }
        if (inputChangelog) {
            inputKinds = new ArrayList<>(rows.size());
            for (BufferedRow row : rows) {
                inputKinds.add(row.value.getRowKind());
            }
        }
        try (ArrowRowDataBatch inputBatch = ArrowRowDataBatch.transpose(values, rowType, allocator);
                NativeDeduplicateResult result = ArrowDeduplicateCDataBridge.execute(
                        nativeHandle, inputBatch, keys, storedRows, inputKinds, allocator)) {
            emit(result);
        }
        rows.clear();
    }

    private void emit(NativeDeduplicateResult result) {
        for (int index = 0; index < result.size(); index++) {
            int inputRow = result.inputRow(index);
            if (inputRow < -1 || inputRow >= rows.size()) {
                throw new IllegalStateException("Native deduplicate returned invalid input-row ordinal " + inputRow);
            }
            BufferedRow source = inputRow >= 0 ? rows.get(inputRow) : null;
            RowData row;
            if (source != null) {
                // The buffered BinaryRow is already an operator-owned copy. Transfer that
                // ownership downstream instead of copying it once here and again at the next
                // serialization boundary.
                row = source.value;
            } else {
                byte[] stored = result.storedRow(index);
                if (stored == null) {
                    throw new IllegalStateException(
                            "Native deduplicate did not return bytes for a stored previous row");
                }
                BinaryRowData binary = new BinaryRowData(rowType.getFieldCount());
                binary.pointTo(MemorySegmentFactory.wrap(stored), 0, stored.length);
                row = serializer.copy(binary);
            }
            row.setRowKind(result.rowKind(index));
            output.collect(
                    source != null && source.hasTimestamp
                            ? new StreamRecord<>(row, source.timestamp)
                            : new StreamRecord<>(row));
        }
    }

    static byte[] createPlan(int[] uniqueKeys, int orderIndex, boolean generateInsert) {
        return createPlan(uniqueKeys, orderIndex, generateInsert, false, false);
    }

    static byte[] createPlan(
            int[] uniqueKeys,
            int orderIndex,
            boolean generateInsert,
            boolean inputChangelog,
            boolean generateUpdateBefore) {
        Deduplicate.Builder deduplicate = Deduplicate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setOrderIndex(orderIndex)
                .setKeepLast(true)
                .setGenerateInsert(generateInsert)
                .setInputChangelog(inputChangelog)
                .setGenerateUpdateBefore(generateUpdateBefore);
        for (int key : uniqueKeys) {
            deduplicate.addKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setDeduplicate(deduplicate))
                .build()
                .toByteArray();
    }

    private byte[] currentBinaryKey() {
        Object current = getCurrentKey();
        if (!(current instanceof BinaryRowData)) {
            throw new IllegalStateException("Native deduplicate requires Flink's BinaryRowData key selector, got "
                    + (current == null ? "null" : current.getClass().getName()));
        }
        BinaryRowData key = (BinaryRowData) current;
        return BinarySegmentUtils.copyToBytes(key.getSegments(), key.getOffset(), key.getSizeInBytes());
    }

    static boolean requiresPreencodedKeys(RowType rowType, int[] uniqueKeys) {
        for (int key : uniqueKeys) {
            LogicalTypeRoot root = rowType.getTypeAt(key).getTypeRoot();
            switch (root) {
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

    static RowType physicalRowType(RowType rowType) {
        List<RowType.RowField> fields = new ArrayList<>(rowType.getFieldCount());
        for (RowType.RowField field : rowType.getFields()) {
            fields.add(new RowType.RowField(
                    field.getName(),
                    physicalType(field.getType()),
                    field.getDescription().orElse(null)));
        }
        return new RowType(rowType.isNullable(), fields);
    }

    private static LogicalType physicalType(LogicalType type) {
        if (type instanceof DistinctType) {
            return physicalType(((DistinctType) type).getSourceType()).copy(type.isNullable());
        }
        if (type instanceof StructuredType) {
            List<LogicalType> types = LogicalTypeChecks.getFieldTypes(type);
            List<String> names = LogicalTypeChecks.getFieldNames(type);
            List<RowType.RowField> fields = new ArrayList<>(types.size());
            for (int index = 0; index < types.size(); index++) {
                fields.add(new RowType.RowField(names.get(index), physicalType(types.get(index))));
            }
            return new RowType(type.isNullable(), fields);
        }
        if (type instanceof ArrayType) {
            return new ArrayType(type.isNullable(), physicalType(((ArrayType) type).getElementType()));
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return new MapType(type.isNullable(), physicalType(map.getKeyType()), physicalType(map.getValueType()));
        }
        if (type instanceof MultisetType) {
            return new MultisetType(type.isNullable(), physicalType(((MultisetType) type).getElementType()));
        }
        if (type instanceof RowType) {
            return physicalRowType((RowType) type);
        }
        return type;
    }

    private static final class BufferedRow {
        private final BinaryRowData value;
        private final byte[] key;
        private final boolean hasTimestamp;
        private final long timestamp;

        private BufferedRow(BinaryRowData value, byte[] key, boolean hasTimestamp, long timestamp) {
            this.value = value;
            this.key = key;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
        }
    }
}
