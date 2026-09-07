/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativePlanState;

/** One Flink checkpoint participant for all independently named state in a native region. */
public final class NativeRegionStateParticipant implements NativeIncrementalStateParticipant {
    private static final int RAW_MAGIC = 0x53465231; // SFR1: node-addressed canonical state.
    private final NativePlanState state;
    private final List<Long> nodeIds;
    private final KeyGroupRange assignedRange;
    private final Path checkpointParent;
    private final NativeMemoryManager memory;

    public NativeRegionStateParticipant(
            NativePlanState state,
            List<Long> nodeIds,
            KeyGroupRange assignedRange,
            Path checkpointParent,
            NativeMemoryManager memory) {
        this.state = java.util.Objects.requireNonNull(state);
        this.nodeIds = nodeIds.stream().sorted().collect(Collectors.toUnmodifiableList());
        if (this.nodeIds.isEmpty()
                || this.nodeIds.get(0) <= 0
                || this.nodeIds.stream().distinct().count() != this.nodeIds.size()
                || assignedRange.getNumberOfKeyGroups() == 0) {
            throw new IllegalArgumentException(
                    "A native region needs unique positive state IDs and assigned key groups");
        }
        this.assignedRange = assignedRange;
        this.checkpointParent =
                java.util.Objects.requireNonNull(checkpointParent).toAbsolutePath();
        this.memory = java.util.Objects.requireNonNull(memory);
    }

    /** Canonical savepoints and memory checkpoints use Flink's raw keyed stream, not Java keyed values. */
    public long writeRawSnapshot(StateSnapshotContext context) throws Exception {
        var keyed = context.getRawKeyedOperatorStateOutput();
        var output = new DataOutputStream(keyed);
        long bytes = 0;
        for (int group : assignedRange) {
            keyed.startNewKeyGroup(group);
            output.writeInt(RAW_MAGIC);
            output.writeInt(nodeIds.size());
            for (long id : nodeIds) output.writeLong(id);
            bytes += 2L * Integer.BYTES + (long) Long.BYTES * nodeIds.size();
            for (long id : nodeIds) {
                byte[] snapshot = state.snapshot(id, group);
                output.writeInt(snapshot.length);
                output.write(snapshot);
                bytes += Integer.BYTES + (long) snapshot.length;
            }
        }
        return bytes;
    }

    /** The owning Flink initialization must fail and discard the region if any frame is invalid. */
    public long restoreRawState(StateInitializationContext context) throws Exception {
        long bytes = 0;
        var restoredGroups = new java.util.HashSet<Integer>();
        for (var provider : context.getRawKeyedStateInputs()) {
            int group = provider.getKeyGroupId();
            if (!assignedRange.contains(group) || !restoredGroups.add(group)) {
                throw new IOException("Unexpected or duplicate native region key group " + group);
            }
            var input = new DataInputStream(provider.getStream());
            if (input.readInt() != RAW_MAGIC || input.readInt() != nodeIds.size()) {
                throw new IOException("Incompatible native region canonical state header");
            }
            for (long id : nodeIds) {
                if (input.readLong() != id) throw new IOException("Native region state node identities changed");
            }
            bytes += 2L * Integer.BYTES + (long) Long.BYTES * nodeIds.size();
            for (long id : nodeIds) {
                int length = input.readInt();
                if (length < 0 || !memory.tryReserve(length)) {
                    throw new IOException("Invalid or unadmitted native region snapshot length " + length);
                }
                try {
                    byte[] snapshot = new byte[length];
                    input.readFully(snapshot);
                    state.restore(id, group, snapshot);
                } finally {
                    memory.release(length);
                }
                bytes += Integer.BYTES + (long) length;
            }
        }
        return bytes;
    }

    @Override
    public Path prepareIncrementalCheckpoint(long checkpointId) throws Exception {
        Path directory = Files.createTempDirectory(checkpointParent, "native-region-" + checkpointId + "-");
        try {
            for (long id : nodeIds) state.checkpoint(id, directory.resolve("node-" + id));
            return directory;
        } catch (Exception | Error failure) {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                    Files.delete(path);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public void restoreIncrementalCheckpoint(Path directory, KeyGroupRange restoredRange) throws Exception {
        if (restoredRange.getNumberOfKeyGroups() == 0
                || !assignedRange.contains(restoredRange.getStartKeyGroup())
                || !assignedRange.contains(restoredRange.getEndKeyGroup())) {
            throw new IOException("Native region checkpoint range is outside the assigned key groups");
        }
        // Validate every namespace before mutating any owner. Flink materializes only safe relative paths.
        try (var paths = Files.list(directory)) {
            var expected = nodeIds.stream().map(id -> "node-" + id).sorted().collect(Collectors.toList());
            var actual =
                    paths.map(path -> path.getFileName().toString()).sorted().collect(Collectors.toList());
            if (!actual.equals(expected))
                throw new IOException("Native region checkpoint state node identities changed");
        }
        for (long id : nodeIds) {
            state.importCheckpoint(
                    id,
                    directory.resolve("node-" + id),
                    restoredRange.getStartKeyGroup(),
                    restoredRange.getEndKeyGroup(),
                    256L * 1024);
        }
    }
}
