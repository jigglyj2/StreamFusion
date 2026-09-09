/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import org.apache.flink.api.common.operators.ProcessingTimeService;

/** Flink schedules the earliest native deadline; all timer output stays in the shared Arrow plan. */
final class NativeRegionProcessingTimeScheduler implements AutoCloseable {
    private final NativeRegionControlScheduler controls;
    private final ProcessingTimeService service;
    private final Supplier<long[]> deadlines;
    private final Set<Long> stages;
    private ScheduledFuture<?> scheduled;
    private Long timestamp;
    private List<Long> owners = List.of();
    private long generation;
    private boolean closed;

    NativeRegionProcessingTimeScheduler(
            NativeRegionControlScheduler controls, ProcessingTimeService service, Supplier<long[]> deadlines) {
        this.controls = controls;
        this.service = service;
        this.deadlines = deadlines;
        this.stages = Set.copyOf(controls.processingTimeStages());
    }

    /** Called after restore and each fully drained native invocation; the native state must be idle. */
    void refresh() {
        refresh(List.of(), 0);
    }

    private void refresh(List<Long> fired, long firedTime) {
        if (closed) return;
        controls.requireHealthy();
        if (stages.isEmpty()) return;
        try {
            long[] values = deadlines.get();
            if (values == null || values.length % 2 != 0 || values.length / 2 > stages.size())
                throw new IllegalArgumentException("Invalid native processing-time deadline snapshot");
            var seen = new HashSet<Long>();
            Long earliest = null;
            var nextOwners = new ArrayList<Long>();
            for (int i = 0; i < values.length; i += 2) {
                long id = values[i];
                long time = values[i + 1];
                if (!stages.contains(id) || !seen.add(id))
                    throw new IllegalArgumentException(
                            "Native processing-time deadlines require unique negotiated stage IDs");
                if (fired.contains(id) && time <= firedTime)
                    throw new IllegalStateException(
                            "Native processing-time callback did not advance its deadline: " + id);
                if (earliest == null || time < earliest) {
                    earliest = time;
                    nextOwners.clear();
                }
                if (time == earliest) nextOwners.add(id);
            }
            owners = List.copyOf(nextOwners);
            if (scheduled != null && earliest != null && earliest.equals(timestamp)) return;
            cancel();
            timestamp = earliest;
            if (earliest != null) {
                long currentGeneration = generation;
                scheduled = service.registerTimer(earliest, time -> fire(currentGeneration, time));
            }
        } catch (RuntimeException | Error failure) {
            controls.invalidate();
            cancel();
            throw failure;
        }
    }

    private void fire(long expectedGeneration, long time) throws Exception {
        if (closed || expectedGeneration != generation) return;
        scheduled = null;
        timestamp = null;
        var due = owners;
        owners = List.of();
        try {
            for (long id : due) controls.processingTime(id, time);
            refresh(due, time);
        } catch (Exception | Error failure) {
            controls.invalidate();
            cancel();
            throw failure;
        }
    }

    private void cancel() {
        generation++;
        if (scheduled != null) scheduled.cancel(false);
        scheduled = null;
        timestamp = null;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        cancel();
        owners = List.of();
    }
}
