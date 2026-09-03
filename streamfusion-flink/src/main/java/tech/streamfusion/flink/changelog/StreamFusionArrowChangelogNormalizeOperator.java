/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.changelog;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowChangelogNormalizeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeChangelogNormalizeBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Native keyed changelog normalization over schema-aware Arrow-row state. */
final class StreamFusionArrowChangelogNormalizeOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;
    private final GeneratedFilterCondition generatedFilter;

    private transient FilterCondition filter;
    private transient FilterContext filterContext;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient Counter expiredStateEntries;
    private transient long[] observedStatistics;

    StreamFusionArrowChangelogNormalizeOperator(
            RowType inputType,
            RowType outputType,
            int[] uniqueKeys,
            boolean generateUpdateBefore,
            long stateTtlMillis,
            RowDataKeySelector keySelector,
            GeneratedFilterCondition generatedFilter) {
        super(
                StreamFusionChangelogNormalizePlan.create(
                        inputType, uniqueKeys, generateUpdateBefore, stateTtlMillis, generatedFilter != null),
                "changelog normalize");
        this.outputType = outputType;
        this.preencodeKeys = requiresPreencodedKeys(inputType, uniqueKeys);
        this.keySelector = keySelector;
        this.generatedFilter = generatedFilter;
    }

    @Override
    public void open() throws Exception {
        super.open();
        currentWatermark = Long.MIN_VALUE;
        if (generatedFilter != null) {
            filter = generatedFilter.newInstance(getRuntimeContext().getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(filter, getRuntimeContext());
            FunctionUtils.openFunction(filter, DefaultOpenContext.INSTANCE);
            filterContext = new FilterContext();
        }
        expiredStateEntries =
                getMetricGroup().addGroup("StreamFusion").counter("changelogNormalizeExpiredStateEntries");
        observedStatistics = NativeChangelogNormalizeBridge.statistics(nativeHandle());
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "changelog normalize") : null;
            boolean[] filterResults = evaluateFilter(input);
            long now = getProcessingTimeService().getCurrentProcessingTime();
            try (ArrowRowDataBatch result = ArrowChangelogNormalizeCDataBridge.execute(
                    nativeHandle(), now, input, keys, filterResults, outputType, allocator(), memoryManager())) {
                int physicalOutput = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutput = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        currentWatermark = mark.getTimestamp();
        recordWatermark();
        output.emitWatermark(mark);
    }

    private boolean[] evaluateFilter(ArrowRowDataBatch input) {
        if (filter == null) {
            return null;
        }
        boolean[] results = new boolean[input.size()];
        for (int row = 0; row < input.size(); row++) {
            filterContext.input = input;
            filterContext.row = row;
            results[row] = filter.apply(filterContext, input.rowView(row));
        }
        filterContext.input = null;
        return results;
    }

    private void updateNativeStatistics() {
        long[] current = NativeChangelogNormalizeBridge.statistics(nativeHandle());
        if (current.length != 3 || observedStatistics.length != 3) {
            throw new IllegalStateException("Native changelog normalize statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        expiredStateEntries.inc(current[2] - observedStatistics[2]);
        observedStatistics = current;
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeChangelogNormalizeBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
    }

    @Override
    protected long createRocksDbHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager) {
        return NativeChangelogNormalizeBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeChangelogNormalizeBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeChangelogNormalizeBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeChangelogNormalizeBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeChangelogNormalizeBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeChangelogNormalizeBridge.destroy(handle);
    }

    @Override
    protected void beforeNativeClose() throws Exception {
        if (filter != null) {
            FunctionUtils.closeFunction(filter);
            filter = null;
        }
    }

    private final class FilterContext implements FilterCondition.Context {
        private final TimerService timerService = new FilterTimerService();
        private ArrowRowDataBatch input;
        private int row;

        @Override
        public Long timestamp() {
            return input.hasTimestamp(row) ? input.timestamp(row) : null;
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }
    }

    private final class FilterTimerService implements TimerService {
        @Override
        public long currentProcessingTime() {
            return getProcessingTimeService().getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(TimerService.UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException(TimerService.UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(TimerService.UNSUPPORTED_DELETE_TIMER_MSG);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException(TimerService.UNSUPPORTED_DELETE_TIMER_MSG);
        }
    }
}
