/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow.writers;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;

/** Direct variable-width buffer writes shared by VARCHAR and VARBINARY source-edge writers. */
final class VariableWidthWriter {
    private final BaseVariableWidthVector vector;
    private ArrowBuf dataBuffer;
    private ArrowBuf offsetBuffer;
    private int dataCapacity;
    private MemorySegment dataSegment;

    VariableWidthWriter(BaseVariableWidthVector vector) {
        this.vector = vector;
        refreshDataBuffer();
    }

    int writeString(int index, int dataOffset, StringData value) {
        if (value instanceof BinaryStringData) {
            BinaryStringData binary = (BinaryStringData) value;
            binary.ensureMaterialized();
            int length = binary.getSizeInBytes();
            ensureDataCapacity(dataOffset, length);
            writeOffsets(index, dataOffset, length);
            copySegments(binary.getSegments(), binary.getOffset(), dataSegment, dataOffset, length);
            vector.setLastSet(index);
            return dataOffset + length;
        }
        return writeBytes(index, dataOffset, value.toBytes());
    }

    int writeBytes(int index, int dataOffset, byte[] value) {
        ensureDataCapacity(dataOffset, value.length);
        writeOffsets(index, dataOffset, value.length);
        dataBuffer.setBytes(dataOffset, value);
        vector.setLastSet(index);
        return dataOffset + value.length;
    }

    void writeNullOffsets(int index, int dataOffset) {
        offsetBuffer.setInt((long) index * Integer.BYTES, dataOffset);
        offsetBuffer.setInt((long) (index + 1) * Integer.BYTES, dataOffset);
        vector.setLastSet(index);
    }

    void refreshDataBuffer() {
        dataBuffer = vector.getDataBuffer();
        offsetBuffer = vector.getOffsetBuffer();
        dataCapacity = Math.toIntExact(dataBuffer.capacity());
        dataSegment = MemorySegmentFactory.wrapOffHeapMemory(dataBuffer.nioBuffer(0, dataCapacity));
    }

    private void ensureDataCapacity(int offset, int length) {
        long required = (long) offset + length;
        if (required > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Arrow variable-width batch exceeds 2 GiB");
        }
        if (required > dataCapacity) {
            vector.reallocDataBuffer(required);
            refreshDataBuffer();
        }
    }

    private void writeOffsets(int index, int offset, int length) {
        offsetBuffer.setInt((long) index * Integer.BYTES, offset);
        offsetBuffer.setInt((long) (index + 1) * Integer.BYTES, offset + length);
    }

    private static void copySegments(
            MemorySegment[] segments, int sourceOffset, MemorySegment target, int targetOffset, int length) {
        int remaining = length;
        int segmentIndex = 0;
        int offset = sourceOffset;
        while (segmentIndex < segments.length && offset >= segments[segmentIndex].size()) {
            offset -= segments[segmentIndex].size();
            segmentIndex++;
        }
        while (remaining > 0) {
            if (segmentIndex >= segments.length) {
                throw new IllegalArgumentException("BinaryStringData segments are shorter than its declared size");
            }
            MemorySegment segment = segments[segmentIndex];
            int copied = Math.min(remaining, segment.size() - offset);
            segment.copyTo(offset, target, targetOffset, copied);
            remaining -= copied;
            targetOffset += copied;
            offset = 0;
            segmentIndex++;
        }
    }
}
