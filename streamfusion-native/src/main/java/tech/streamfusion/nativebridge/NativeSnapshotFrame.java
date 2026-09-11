/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.io.DataInputStream;
import java.io.IOException;

/** SFR1 node frames retain Int32 lengths; -1 explicitly selects the Int64 frame extension. */
final class NativeSnapshotFrame {
    private NativeSnapshotFrame() {}

    static long readLength(DataInputStream input) throws IOException {
        int prefix = input.readInt();
        if (prefix >= 0) return prefix;
        if (prefix != -1) throw new IOException("Unknown native snapshot frame extension " + prefix);
        long length = input.readLong();
        if (length <= Integer.MAX_VALUE || length > Long.MAX_VALUE - 12)
            throw new IOException("Invalid extended native snapshot length " + length);
        return length;
    }

    static long framedBytes(long length) {
        return Math.addExact(length, length <= Integer.MAX_VALUE ? 4 : 12);
    }
}
