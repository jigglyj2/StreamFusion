/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.nio.file.Path;
import org.apache.flink.runtime.state.KeyGroupRange;

/** Native RocksDB state owned by a StreamFusion operator and checkpointed by Flink. */
public interface NativeIncrementalStateParticipant {
    /** Creates a stable local RocksDB checkpoint before asynchronous upload starts. */
    Path prepareIncrementalCheckpoint(long checkpointId) throws Exception;

    /** Records exact upload and shared-state reuse after Flink materializes an incremental handle. */
    default void completeIncrementalCheckpoint(long checkpointId, long uploadedBytes, long reusedBytes) {}

    /** Records an incremental checkpoint failure. */
    default void failIncrementalCheckpoint(long checkpointId) {}

    /** Imports one restored RocksDB checkpoint, restricted to the assigned key-group range. */
    void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange keyGroupRange) throws Exception;
}
