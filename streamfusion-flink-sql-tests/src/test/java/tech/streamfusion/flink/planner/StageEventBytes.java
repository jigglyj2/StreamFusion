/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.io.IOException;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Test-sink serialization of complete changelogs, record envelopes and ordered controls. */
final class StageEventBytes {
    private StageEventBytes() {}

    static void row(RowType type, RowData row, boolean present, long timestamp, DataOutputSerializer output)
            throws IOException {
        output.writeByte(0);
        new RowDataSerializer(type).serialize(row, output);
        output.writeBoolean(present);
        if (present) output.writeLong(timestamp);
    }

    static void encode(RowType type, StreamElement event, DataOutputSerializer output) throws IOException {
        if (event instanceof StreamRecord) {
            var record = (StreamRecord<?>) event;
            row(type, (RowData) record.getValue(), record.hasTimestamp(), record.getTimestamp(), output);
        } else if (event instanceof Watermark) {
            output.writeByte(1);
            output.writeLong(((Watermark) event).getTimestamp());
        } else if (event instanceof WatermarkStatus) {
            output.writeByte(2);
            output.writeBoolean(((WatermarkStatus) event).isIdle());
        } else if (event instanceof LatencyMarker) {
            var marker = (LatencyMarker) event;
            output.writeByte(3);
            output.writeLong(marker.getMarkedTime());
            output.write(marker.getOperatorId().getBytes());
            output.writeInt(marker.getSubtaskIndex());
        } else throw new AssertionError("Uncovered control " + event);
    }
}
