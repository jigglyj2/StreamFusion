/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/** Common task-control view over Flink's actual regular and multiple-input join harnesses. */
interface FlinkJoinMetricOracle extends AutoCloseable {
    InternalOperatorMetricGroup group();

    void accept(int port, StreamElement event) throws Exception;

    List<StreamElement> drain();

    void prepareSnapshotPreBarrier(long checkpoint) throws Exception;
}
