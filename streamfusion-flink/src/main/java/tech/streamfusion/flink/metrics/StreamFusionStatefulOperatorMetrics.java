/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Common processing, changelog, state, checkpoint, and restore metrics for native keyed operators. */
public final class StreamFusionStatefulOperatorMetrics {
    private final Counter processedBatches;
    private final Counter processedRows;
    private final Counter emittedRows;
    private final Counter emittedInserts;
    private final Counter emittedUpdateBefores;
    private final Counter emittedUpdateAfters;
    private final Counter emittedDeletes;
    private final Counter stateReadBatches;
    private final Counter stateWriteBatches;
    private final Counter processingFailures;
    private final Counter watermarksAdvanced;
    private final Counter eventTimeTimersFired;
    private final Counter processingTimeTimersFired;
    private final Counter timersRegistered;
    private final Counter timersDeleted;
    private final Counter timersFired;
    private final Counter checkpoints;
    private final Counter alignedCheckpoints;
    private final Counter unalignedCheckpoints;
    private final Counter canonicalSavepoints;
    private final Counter incrementalCheckpoints;
    private final Counter checkpointBytes;
    private final Counter incrementalUploadedBytes;
    private final Counter incrementalReusedBytes;
    private final Counter checkpointDurationNanos;
    private final Counter checkpointFailures;
    private final Counter restores;
    private final Counter restoreBytes;
    private final Counter restoreDurationNanos;
    private final Counter restoreFailures;

    public StreamFusionStatefulOperatorMetrics(MetricGroup operatorMetricGroup, boolean rocksDb) {
        MetricGroup metrics = operatorMetricGroup.addGroup("StreamFusion");
        processedBatches = metrics.counter("processedBatches");
        processedRows = metrics.counter("processedRows");
        emittedRows = metrics.counter("emittedRows");
        emittedInserts = metrics.counter("emittedInserts");
        emittedUpdateBefores = metrics.counter("emittedUpdateBefores");
        emittedUpdateAfters = metrics.counter("emittedUpdateAfters");
        emittedDeletes = metrics.counter("emittedDeletes");
        stateReadBatches = metrics.counter("stateReadBatches");
        stateWriteBatches = metrics.counter("stateWriteBatches");
        processingFailures = metrics.counter("processingFailures");
        watermarksAdvanced = metrics.counter("watermarksAdvanced");
        eventTimeTimersFired = metrics.counter("eventTimeTimersFired");
        processingTimeTimersFired = metrics.counter("processingTimeTimersFired");
        timersRegistered = metrics.counter("timersRegistered");
        timersDeleted = metrics.counter("timersDeleted");
        timersFired = metrics.counter("timersFired");
        checkpoints = metrics.counter("checkpoints");
        alignedCheckpoints = metrics.counter("alignedCheckpoints");
        unalignedCheckpoints = metrics.counter("unalignedCheckpoints");
        canonicalSavepoints = metrics.counter("canonicalSavepoints");
        incrementalCheckpoints = metrics.counter("incrementalCheckpoints");
        checkpointBytes = metrics.counter("checkpointBytes");
        incrementalUploadedBytes = metrics.counter("incrementalUploadedBytes");
        incrementalReusedBytes = metrics.counter("incrementalReusedBytes");
        checkpointDurationNanos = metrics.counter("checkpointDurationNanos");
        checkpointFailures = metrics.counter("checkpointFailures");
        restores = metrics.counter("restores");
        restoreBytes = metrics.counter("restoreBytes");
        restoreDurationNanos = metrics.counter("restoreDurationNanos");
        restoreFailures = metrics.counter("restoreFailures");
        metrics.gauge("rocksDbBackend", () -> rocksDb ? 1 : 0);
    }

    public void processed(ArrowRowDataBatch input, ArrowRowDataBatch output) {
        processedWithoutStateCalls(input, output);
        stateReadBatches.inc();
        stateWriteBatches.inc();
    }

    public void processedWithoutStateCalls(ArrowRowDataBatch input, ArrowRowDataBatch output) {
        processedBatches.inc();
        processedRows.inc(input.size());
        emittedRows.inc(output.size());
        for (int row = 0; row < output.size(); row++) {
            RowKind kind = output.rowKind(row);
            switch (kind) {
                case INSERT:
                    emittedInserts.inc();
                    break;
                case UPDATE_BEFORE:
                    emittedUpdateBefores.inc();
                    break;
                case UPDATE_AFTER:
                    emittedUpdateAfters.inc();
                    break;
                case DELETE:
                    emittedDeletes.inc();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flink row kind " + kind);
            }
        }
    }

    public void nativeWindowStatistics(long stateReads, long stateWrites, long registered, long deleted, long fired) {
        stateReadBatches.inc(stateReads);
        stateWriteBatches.inc(stateWrites);
        timersRegistered.inc(registered);
        timersDeleted.inc(deleted);
        timersFired.inc(fired);
    }

    public void processingFailed() {
        processingFailures.inc();
    }

    public void watermarkAdvanced() {
        watermarksAdvanced.inc();
    }

    public void timerOutput(ArrowRowDataBatch output, boolean processingTime) {
        emittedRows.inc(output.size());
        if (processingTime) {
            processingTimeTimersFired.inc(output.size());
        } else {
            eventTimeTimersFired.inc(output.size());
        }
        for (int row = 0; row < output.size(); row++) {
            switch (output.rowKind(row)) {
                case INSERT:
                    emittedInserts.inc();
                    break;
                case UPDATE_BEFORE:
                    emittedUpdateBefores.inc();
                    break;
                case UPDATE_AFTER:
                    emittedUpdateAfters.inc();
                    break;
                case DELETE:
                    emittedDeletes.inc();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flink row kind " + output.rowKind(row));
            }
        }
    }

    public void checkpointCompleted(
            CheckpointOptions options, long bytes, long uploadedBytes, long reusedBytes, long durationNanos) {
        checkpoints.inc();
        checkpointBytes.inc(bytes);
        checkpointDurationNanos.inc(durationNanos);
        if (options.isUnalignedCheckpoint()) {
            unalignedCheckpoints.inc();
        } else if (!options.getCheckpointType().isSavepoint()) {
            alignedCheckpoints.inc();
        }
        if (options.getCheckpointType() instanceof SavepointType
                && ((SavepointType) options.getCheckpointType()).getFormatType() == SavepointFormatType.CANONICAL) {
            canonicalSavepoints.inc();
        }
        if (uploadedBytes >= 0) {
            incrementalCheckpoints.inc();
            incrementalUploadedBytes.inc(uploadedBytes);
            incrementalReusedBytes.inc(reusedBytes);
        }
    }

    public void checkpointFailed() {
        checkpointFailures.inc();
    }

    public void restored(long bytes, long durationNanos) {
        restores.inc();
        restoreBytes.inc(bytes);
        restoreDurationNanos.inc(durationNanos);
    }

    public void restoreFailed() {
        restoreFailures.inc();
    }
}
