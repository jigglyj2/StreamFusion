/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Opt-in, release-native Q18 RowData CPU profiling target. */
@EnabledIfSystemProperty(named = "streamfusion.q18.benchmark", matches = "true")
class StreamFusionDeduplicateQ18BenchmarkTest {
    private static final int MAX_PARALLELISM = 128;
    private static final int ORDER_INDEX = 5;
    private static final int[] UNIQUE_KEYS = {1, 0};
    private static final StringData CHANNEL = StringData.fromString("channel");
    private static final StringData URL = StringData.fromString("url");
    private static final StringData EXTRA = StringData.fromString("extra");
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new BigIntType(false),
                new BigIntType(false),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new TimestampType(false, TimestampKind.ROWTIME, 3),
                new VarCharType(false, VarCharType.MAX_LENGTH)
            },
            new String[] {"auction", "bidder", "price", "channel", "url", "dateTime", "extra"});
    private static final InternalTypeInfo<RowData> INPUT_INFO = InternalTypeInfo.of(INPUT_TYPE);

    @Test
    void profileQ18RowDataPath() throws Exception {
        String engine = System.getProperty("streamfusion.q18.engine", "streamfusion");
        String backend = System.getProperty("streamfusion.q18.backend", "hashmap");
        int warmupEvents = Integer.getInteger("streamfusion.q18.warmup-events", 100_000);
        int events = Integer.getInteger("streamfusion.q18.events", 2_000_000);
        int iterations = Integer.getInteger("streamfusion.q18.iterations", 1);
        if (iterations <= 0) {
            throw new IllegalArgumentException("Q18 benchmark iterations must be positive");
        }

        run(engine, backend, warmupEvents);
        String recordingPath = System.getProperty("streamfusion.q18.jfr");
        long checksum = Long.MIN_VALUE;
        long[] elapsedNanos = new long[iterations];
        try (Recording recording =
                recordingPath == null ? null : new Recording(Configuration.getConfiguration("profile"))) {
            if (recording != null) {
                recording.start();
            }
            for (int iteration = 0; iteration < iterations; iteration++) {
                long start = System.nanoTime();
                long iterationChecksum = run(engine, backend, events);
                elapsedNanos[iteration] = System.nanoTime() - start;
                if (checksum != Long.MIN_VALUE && checksum != iterationChecksum) {
                    throw new IllegalStateException("Q18 benchmark checksum changed between iterations");
                }
                checksum = iterationChecksum;
                System.out.printf(
                        Locale.ROOT,
                        "Q18_PROFILE_ITERATION engine=%s backend=%s iteration=%d events=%d seconds=%.6f records_per_second=%.2f checksum=%d%n",
                        engine,
                        backend,
                        iteration + 1,
                        events,
                        elapsedNanos[iteration] / 1_000_000_000.0,
                        events * 1_000_000_000.0 / elapsedNanos[iteration],
                        checksum);
            }
            if (recording != null) {
                recording.stop();
                recording.dump(Path.of(recordingPath));
            }
        }
        long[] sortedNanos = elapsedNanos.clone();
        Arrays.sort(sortedNanos);
        double medianSeconds = sortedNanos[sortedNanos.length / 2] / 1_000_000_000.0;
        System.out.printf(
                Locale.ROOT,
                "Q18_PROFILE engine=%s backend=%s iterations=%d events=%d median_seconds=%.6f median_records_per_second=%.2f checksum=%d%n",
                engine,
                backend,
                iterations,
                events,
                medianSeconds,
                events / medianSeconds,
                checksum);
    }

    private static long run(String engine, String backend, int events) throws Exception {
        Q18KeySelector selector = new Q18KeySelector();
        OneInputStreamOperator<RowData, RowData> operator;
        boolean streamFusion;
        if ("streamfusion".equals(engine)) {
            streamFusion = true;
            operator = new StreamFusionDeduplicateOperator(INPUT_TYPE, UNIQUE_KEYS, ORDER_INDEX, true);
        } else if ("flink".equals(engine)) {
            streamFusion = false;
            operator = new KeyedProcessOperator<>(
                    new RowTimeDeduplicateFunction(INPUT_INFO, 0L, ORDER_INDEX, false, true, true));
        } else {
            throw new IllegalArgumentException("Unknown Q18 benchmark engine: " + engine);
        }

        long managedMemory = Long.getLong("streamfusion.q18.managed-memory-bytes", 512L << 20);
        try (MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Q18 profile")
                        .setManagedMemorySize(managedMemory)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .build();
                KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                operator, selector, selector.getProducedType(), environment)) {
            if ("rocksdb".equals(backend)) {
                EmbeddedRocksDBStateBackend rocksDb = new EmbeddedRocksDBStateBackend(true);
                harness.setStateBackend(streamFusion ? new StreamFusionStateBackend(rocksDb) : rocksDb);
            } else if ("hashmap".equals(backend)) {
                HashMapStateBackend hashMap = new HashMapStateBackend();
                harness.setStateBackend(streamFusion ? new StreamFusionStateBackend(hashMap) : hashMap);
            } else {
                throw new IllegalArgumentException("Unknown Q18 benchmark backend: " + backend);
            }
            harness.setup(INPUT_INFO.toSerializer());
            harness.open();
            long checksum = 0;
            for (int index = 0; index < events; index++) {
                GenericRowData row = row(index);
                RowData input = streamFusion
                        ? row
                        : INPUT_INFO.toRowSerializer().toBinaryRow(row).copy();
                harness.processElement(new StreamRecord<>(input, index));
                if ((index & 1_023) == 1_023) {
                    checksum += drain(harness);
                }
            }
            harness.processWatermark(new Watermark(Long.MAX_VALUE));
            return checksum + drain(harness);
        }
    }

    private static GenericRowData row(int index) {
        long mixed = mix(index);
        GenericRowData row = new GenericRowData(RowKind.INSERT, 7);
        row.setField(0, Math.floorMod(mixed, 100_000L));
        row.setField(1, Math.floorMod(mixed >>> 17, 20_000L));
        row.setField(2, (long) index);
        row.setField(3, CHANNEL);
        row.setField(4, URL);
        row.setField(5, TimestampData.fromEpochMillis(index));
        row.setField(6, EXTRA);
        return row;
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static long drain(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        long checksum = 0;
        Object output;
        while ((output = harness.getOutput().poll()) != null) {
            if (output instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                StreamRecord<RowData> record = (StreamRecord<RowData>) output;
                checksum += record.getValue().getLong(2);
            }
        }
        return checksum;
    }

    private static final class Q18KeySelector implements RowDataKeySelector {
        private static final long serialVersionUID = 1L;
        private static final InternalTypeInfo<RowData> KEY_TYPE =
                InternalTypeInfo.ofFields(new BigIntType(false), new BigIntType(false));

        @Override
        public RowData getKey(RowData value) {
            BinaryRowData key = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(key);
            writer.writeLong(0, value.getLong(1));
            writer.writeLong(1, value.getLong(0));
            writer.complete();
            return key;
        }

        @Override
        public InternalTypeInfo<RowData> getProducedType() {
            return KEY_TYPE;
        }

        @Override
        public RowDataKeySelector copy() {
            return new Q18KeySelector();
        }
    }
}
