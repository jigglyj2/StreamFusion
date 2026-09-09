/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

/** Flink TimerHeapInternalTimer compares timestamps only; equal-time keys have no defined order. */
final class WindowTimerEventBytes {
    private WindowTimerEventBytes() {}

    static byte[] canonical(org.apache.flink.table.types.logical.RowType type, int endIndex, byte[] bytes)
            throws Exception {
        var input = new DataInputDeserializer(bytes);
        var output = new DataOutputSerializer(Math.max(1, bytes.length));
        var rows = new ArrayList<byte[]>();
        var serializer = new RowDataSerializer(type);
        Long frontier = null;
        while (input.available() > 0) {
            int start = input.getPosition();
            int kind = input.readUnsignedByte();
            if (kind == 0) {
                var row = serializer.deserialize(input);
                if (row.getRowKind() != RowKind.INSERT) throw new AssertionError("append-only window must emit INSERT");
                if (input.readBoolean()) input.readLong();
                long end = row.getTimestamp(endIndex, 3).getMillisecond();
                if (frontier != null && frontier != end) flush(rows, output);
                frontier = end;
                rows.add(Arrays.copyOfRange(bytes, start, input.getPosition()));
            } else {
                flush(rows, output);
                frontier = null;
                switch (kind) {
                    case 1:
                        input.readLong();
                        break;
                    case 2:
                        input.readBoolean();
                        break;
                    case 3:
                        input.skipBytesToRead(28);
                        break;
                    default:
                        throw new AssertionError("Unknown control " + kind);
                }
                output.write(bytes, start, input.getPosition() - start);
            }
        }
        flush(rows, output);
        return output.getCopyOfBuffer();
    }

    private static void flush(List<byte[]> rows, DataOutputSerializer output) throws Exception {
        rows.sort(Arrays::compareUnsigned);
        for (var row : rows) output.write(row);
        rows.clear();
    }
}
