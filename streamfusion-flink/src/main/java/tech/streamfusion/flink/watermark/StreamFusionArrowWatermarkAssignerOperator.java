/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.watermark;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Flink's watermark state machine over Arrow batches, without a RowData operator boundary. */
final class StreamFusionArrowWatermarkAssignerOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, ProcessingTimeCallback {
    private final int rowtimeFieldIndex;
    private final long idleTimeout;
    private final WatermarkGenerator watermarkGenerator;
    private transient long lastWatermark;
    private transient long watermarkInterval;
    private transient long timerInterval;
    private transient long currentWatermark;
    private transient long lastWatermarkPeriodicEmitTime;
    private transient long timeSinceLastIdleCheck;
    private transient WatermarkStatus currentStatus = WatermarkStatus.ACTIVE;
    private transient long processedElements;
    private transient long lastIdleCheckProcessedElements = -1;
    private transient PausableRelativeClock inputActivityClock;
    private transient FlinkManagedMemory managedMemory;

    StreamFusionArrowWatermarkAssignerOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            int rowtimeFieldIndex,
            WatermarkGenerator watermarkGenerator,
            long idleTimeout,
            ProcessingTimeService processingTimeService) {
        super(parameters);
        this.rowtimeFieldIndex = rowtimeFieldIndex;
        this.watermarkGenerator = watermarkGenerator;
        this.idleTimeout = idleTimeout;
        this.processingTimeService = processingTimeService;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-arrow-watermark");
        inputActivityClock =
                new PausableRelativeClock(getProcessingTimeService().getClock());
        getContainingTask()
                .getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .registerBackPressureListener(inputActivityClock);
        currentWatermark = 0;
        watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        lastWatermarkPeriodicEmitTime = now;
        timeSinceLastIdleCheck = now;
        if (watermarkInterval > 0 || idleTimeout > 0) {
            timerInterval = calculateTimerInterval(watermarkInterval, idleTimeout);
            getProcessingTimeService().registerTimer(now + timerInterval, this);
        }
        FunctionUtils.setFunctionRuntimeContext(watermarkGenerator, getRuntimeContext());
        FunctionUtils.openFunction(watermarkGenerator, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch batch = element.getValue();
        if (isIdlenessEnabled() && WatermarkStatus.IDLE.equals(currentStatus)) {
            emitWatermarkStatus(WatermarkStatus.ACTIVE);
        }
        int rangeStart = 0;
        int emittedBatches = 0;
        for (int rowIndex = 0; rowIndex < batch.size(); rowIndex++) {
            processedElements++;
            RowData row = batch.rowView(rowIndex);
            if (row.isNullAt(rowtimeFieldIndex)) {
                throw new RuntimeException(
                        "RowTime field should not be null, please convert it to a non-null long value.");
            }
            Long candidate = watermarkGenerator.currentWatermark(row);
            if (candidate != null) {
                currentWatermark = Math.max(currentWatermark, candidate);
            }
            if (currentWatermark - lastWatermark > watermarkInterval) {
                emitRange(batch, rangeStart, rowIndex + 1 - rangeStart);
                emittedBatches++;
                rangeStart = rowIndex + 1;
                advanceWatermark();
            }
        }
        if (rangeStart < batch.size()) {
            emitRange(batch, rangeStart, batch.size() - rangeStart);
            emittedBatches++;
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), emittedBatches, batch.size());
    }

    private void emitRange(ArrowRowDataBatch batch, int offset, int length) {
        if (offset == 0 && length == batch.size()) {
            output.collect(new StreamRecord<>(batch));
            return;
        }
        try (ArrowRowDataBatch slice = batch.slice(offset, length, managedMemory.allocator())) {
            output.collect(new StreamRecord<>(slice));
        }
    }

    private void advanceWatermark() {
        if (currentWatermark > lastWatermark) {
            lastWatermark = currentWatermark;
            output.emitWatermark(new Watermark(currentWatermark));
        }
    }

    @Override
    public void onProcessingTime(long timestamp) {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long activityNow = inputActivityClock.relativeTimeMillis();
        if (watermarkInterval > 0 && lastWatermarkPeriodicEmitTime + watermarkInterval <= now) {
            lastWatermarkPeriodicEmitTime = now;
            advanceWatermark();
        }
        if (processedElements != lastIdleCheckProcessedElements) {
            timeSinceLastIdleCheck = activityNow;
            lastIdleCheckProcessedElements = processedElements;
        }
        if (isIdlenessEnabled()
                && WatermarkStatus.ACTIVE.equals(currentStatus)
                && timeSinceLastIdleCheck + idleTimeout <= activityNow) {
            emitWatermarkStatus(WatermarkStatus.IDLE);
        }
        getProcessingTimeService().registerTimer(now + timerInterval, this);
    }

    @Override
    public void processWatermark(Watermark watermark) {
        if (watermark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
            if (isIdlenessEnabled() && WatermarkStatus.IDLE.equals(currentStatus)) {
                emitWatermarkStatus(WatermarkStatus.ACTIVE);
            }
            currentWatermark = Long.MAX_VALUE;
            output.emitWatermark(watermark);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) {
        emitWatermarkStatus(watermarkStatus);
    }

    private void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        currentStatus = watermarkStatus;
        output.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void finish() {
        processWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    public void close() throws Exception {
        try {
            if (inputActivityClock != null) {
                getContainingTask()
                        .getEnvironment()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .unregisterBackPressureListener(inputActivityClock);
            }
            FunctionUtils.closeFunction(watermarkGenerator);
            if (managedMemory != null) {
                managedMemory.close();
                managedMemory = null;
            }
        } finally {
            super.close();
        }
    }

    private boolean isIdlenessEnabled() {
        return idleTimeout > 0;
    }

    private static long calculateTimerInterval(long watermarkInterval, long idleTimeout) {
        if (watermarkInterval <= 0) {
            return idleTimeout;
        }
        if (idleTimeout <= 0) {
            return watermarkInterval;
        }
        long smaller = Math.min(watermarkInterval, idleTimeout);
        long larger = Math.max(watermarkInterval, idleTimeout);
        return Math.max(smaller * 5 < larger ? smaller : smaller / 5, 1);
    }
}
