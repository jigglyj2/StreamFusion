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

    /** Imports one restored RocksDB checkpoint, restricted to the assigned key-group range. */
    void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange keyGroupRange) throws Exception;
}
