/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.union;

import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;

/** SQL UNION is input wiring: its channel merge uses Flink's valve, not an operator combiner. */
public final class UnionInputWatermarks implements PushingAsyncDataInput.DataOutput<Void> {
    private final StatusWatermarkValve valve;
    private final ThrowingConsumer<Watermark, Exception> watermarks;
    private final ThrowingConsumer<WatermarkStatus, Exception> statuses;

    public UnionInputWatermarks(
            int inputs,
            ThrowingConsumer<Watermark, Exception> watermarks,
            ThrowingConsumer<WatermarkStatus, Exception> statuses) {
        valve = new StatusWatermarkValve(inputs);
        this.watermarks = watermarks;
        this.statuses = statuses;
    }

    public void watermark(int input, long timestamp) throws Exception {
        valve.inputWatermark(new Watermark(timestamp), input, this);
    }

    public void status(int input, WatermarkStatus status) throws Exception {
        valve.inputWatermarkStatus(status, input, this);
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        watermarks.accept(watermark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus status) throws Exception {
        statuses.accept(status);
    }

    @Override
    public void emitRecord(StreamRecord<Void> record) {
        throw new UnsupportedOperationException("A UNION watermark valve cannot emit data");
    }

    @Override
    public void emitLatencyMarker(LatencyMarker marker) {
        throw new UnsupportedOperationException("Latency markers bypass watermark merging");
    }

    @Override
    public void emitRecordAttributes(RecordAttributes attributes) {
        throw new UnsupportedOperationException("Record attributes bypass watermark merging");
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) {
        throw new UnsupportedOperationException("Generalized watermarks require their own Flink contract");
    }
}
