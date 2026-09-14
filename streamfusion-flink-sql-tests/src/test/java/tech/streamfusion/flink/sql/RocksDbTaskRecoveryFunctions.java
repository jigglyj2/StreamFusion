/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Checkpointed source position and complete changelog capture on Flink's ordinary boundaries. */
final class RocksDbTaskRecoveryFunctions {
    static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT =
            Types.ROW_NAMED(new String[] {"category", "amount"}, Types.STRING, Types.LONG);
    static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> OUTPUT =
            Types.ROW(Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG);

    private RocksDbTaskRecoveryFunctions() {}

    static final class Source extends RichParallelSourceFunction<Row>
            implements CheckpointedFunction, CheckpointListener {
        private final String run;
        private transient ListState<Integer> offsets;
        private int next;
        private volatile boolean running = true;
        private volatile long captured = -1;
        private volatile long completed = -1;

        Source(String run) {
            this.run = run;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            offsets = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("offset", Types.INT));
            if (context.isRestored()) {
                for (int value : offsets.get()) next = value;
                RocksDbTaskRecoveryControl.RUNS.get(run).restoredSourceOffsets.add(next);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsets.update(List.of(next));
            // Require the complete prefix to reach the sink before selecting the checkpoint.
            // This proves restore reads nonempty aggregate SSTs, rather than only replaying input.
            if (next == 80 && RocksDbTaskRecoveryControl.RUNS.get(run).observedRows == 154)
                captured = context.getCheckpointId();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (captured == checkpointId) completed = checkpointId;
        }

        @Override
        public void run(SourceContext<Row> context) throws Exception {
            while (running && next < 160) {
                if (next == 80 && getRuntimeContext().getTaskInfo().getAttemptNumber() == 0) {
                    if (completed < 0) {
                        Thread.sleep(10);
                        continue;
                    }
                    var control = RocksDbTaskRecoveryControl.RUNS.get(run);
                    control.beforeFailure(completed);
                    control.completedCheckpointFailures++;
                    throw new Exception("Intentional failure after a globally completed checkpoint");
                }
                synchronized (context.getCheckpointLock()) {
                    int phase = next / 40;
                    int index = next % 40;
                    // Retractions cross the checkpoint: phase 2 removes the exact phase 1 rows.
                    int generation = phase == 2 ? 1 : phase;
                    var random = new java.util.Random(generation * 1009L + index);
                    Row row = Row.of(
                            index % 7 == 0 ? null : "key-" + index % 5,
                            index % 9 == 0 ? null : (long) random.nextInt(200) - 100);
                    row.setKind(phase == 2 ? RowKind.DELETE : RowKind.INSERT);
                    context.collect(row);
                    next++;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    static final class Sink extends RichSinkFunction<Row> implements CheckpointedFunction {
        private final String run;
        private final List<Row> rows = new ArrayList<>();
        private transient ListState<Row> state;

        Sink(String run) {
            this.run = run;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("changelog", OUTPUT));
            if (context.isRestored()) for (Row row : state.get()) rows.add(Row.copy(row));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(rows);
        }

        @Override
        public void invoke(Row row, Context context) {
            rows.add(Row.copy(row));
            RocksDbTaskRecoveryControl.RUNS.get(run).observedRows = rows.size();
        }

        @Override
        public void finish() {
            RocksDbTaskRecoveryControl.RUNS.get(run).result = new ArrayList<>(rows);
        }
    }
}
