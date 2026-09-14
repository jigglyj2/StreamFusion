/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.state.AbstractIncrementalStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

final class NativeCheckpointMetadata {
    private static final byte[] META_MAGIC = new byte[] {'S', 'F', 'I', '1'};

    private NativeCheckpointMetadata() {}

    static boolean isNativeHandle(AbstractIncrementalStateHandle handle) {
        return isNativeHandle(handle, null);
    }

    static boolean isNativeHandle(
            AbstractIncrementalStateHandle handle, org.apache.flink.core.fs.CloseableRegistry cancellation) {
        StreamStateHandle metadata = handle.getMetaDataStateHandle();
        try {
            byte[] bytes;
            if (metadata.asBytesIfInMemory().isPresent()) {
                bytes = metadata.asBytesIfInMemory().get();
            } else {
                try (InputStream input = metadata.openInputStream()) {
                    if (cancellation != null) cancellation.registerCloseable(input);
                    try {
                        bytes = input.readNBytes(META_MAGIC.length);
                    } finally {
                        if (cancellation != null) cancellation.unregisterCloseable(input);
                    }
                }
            }
            return bytes.length >= META_MAGIC.length
                    && java.util.Arrays.equals(java.util.Arrays.copyOf(bytes, META_MAGIC.length), META_MAGIC);
        } catch (IOException ignored) {
            return false;
        }
    }

    static byte[] encode(List<String> emptyFiles) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.write(META_MAGIC);
            output.writeInt(emptyFiles.size());
            for (String path : emptyFiles) {
                byte[] encoded = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                output.writeInt(encoded.length);
                output.write(encoded);
            }
        }
        return bytes.toByteArray();
    }

    static List<String> emptyFiles(StreamStateHandle metadata) throws IOException {
        return emptyFiles(metadata, null);
    }

    static List<String> emptyFiles(StreamStateHandle metadata, org.apache.flink.core.fs.CloseableRegistry cancellation)
            throws IOException {
        InputStream stream = metadata.asBytesIfInMemory().isPresent()
                ? new ByteArrayInputStream(metadata.asBytesIfInMemory().get())
                : metadata.openInputStream();
        if (cancellation != null) cancellation.registerCloseable(stream);
        try (DataInputStream input = new DataInputStream(stream)) {
            byte[] magic = new byte[META_MAGIC.length];
            input.readFully(magic);
            if (!java.util.Arrays.equals(magic, META_MAGIC)) {
                throw new IOException("Not a StreamFusion incremental RocksDB manifest");
            }
            int count = input.readInt();
            if (count < 0 || count > 10_000) {
                throw new IOException("Invalid empty-file count in native RocksDB manifest");
            }
            List<String> paths = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                int length = input.readInt();
                if (length < 0 || length > 1 << 20) {
                    throw new IOException("Invalid path length in native RocksDB manifest");
                }
                byte[] path = new byte[length];
                input.readFully(path);
                paths.add(new String(path, java.nio.charset.StandardCharsets.UTF_8));
            }
            return paths;
        } finally {
            if (cancellation != null) cancellation.unregisterCloseable(stream);
        }
    }
}
