/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class NativeSnapshotFrameTest {
    @Test
    void readsOriginalAndExplicitlyVersionedLargeFrames() throws Exception {
        for (long length :
                new long[] {0, 16, Integer.MAX_VALUE, 1L + Integer.MAX_VALUE, 5L << 30, Long.MAX_VALUE - 12}) {
            var bytes = new ByteArrayOutputStream();
            var output = new DataOutputStream(bytes);
            if (length <= Integer.MAX_VALUE) output.writeInt((int) length);
            else {
                output.writeInt(-1);
                output.writeLong(length);
            }
            var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
            assertThat(NativeSnapshotFrame.readLength(input)).isEqualTo(length);
            assertThat(NativeSnapshotFrame.framedBytes(length)).isEqualTo(length + bytes.size());
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void rejectsUnknownTruncatedNoncanonicalAndOverflowingFrameExtensions() throws Exception {
        for (long length : new long[] {-1, 0, Integer.MAX_VALUE, Long.MAX_VALUE}) {
            var bytes = new ByteArrayOutputStream();
            var output = new DataOutputStream(bytes);
            output.writeInt(-1);
            output.writeLong(length);
            assertThatThrownBy(() -> NativeSnapshotFrame.readLength(
                            new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))))
                    .isInstanceOf(IOException.class);
        }
        for (byte[] bytes : new byte[][] {new byte[] {-1, -1, -1, -2}, new byte[] {-1, -1, -1, -1, 0}}) {
            assertThatThrownBy(
                            () -> NativeSnapshotFrame.readLength(new DataInputStream(new ByteArrayInputStream(bytes))))
                    .isInstanceOf(IOException.class);
        }
    }
}
