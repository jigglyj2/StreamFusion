package tech.streamfusion.benchmark.nexmark;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Process-local, deterministic result collection for the Kafka-free RowData benchmark. */
final class BenchmarkResultStore {
    private static final Map<String, ConcurrentLinkedQueue<RecordedRow>> RUNS = new ConcurrentHashMap<>();

    private BenchmarkResultStore() {}

    static void begin(String runId) {
        if (RUNS.putIfAbsent(runId, new ConcurrentLinkedQueue<>()) != null) {
            throw new IllegalStateException("Benchmark result run already exists: " + runId);
        }
    }

    static void add(
            String runId,
            byte[] changelogRow,
            byte[] materializedRow,
            byte[] materializationKey,
            boolean accumulate,
            String debug,
            String materializedDebug) {
        ConcurrentLinkedQueue<RecordedRow> rows = RUNS.get(runId);
        if (rows == null) {
            throw new IllegalStateException("Unknown benchmark result run: " + runId);
        }
        rows.add(new RecordedRow(
                changelogRow, materializedRow, materializationKey, accumulate, debug, materializedDebug));
    }

    static Result finish(String runId) {
        ConcurrentLinkedQueue<RecordedRow> collected = RUNS.remove(runId);
        if (collected == null) {
            throw new IllegalStateException("Unknown benchmark result run: " + runId);
        }
        List<RecordedRow> rows = new ArrayList<>(collected);
        TreeMap<byte[], MaterializedRow> materialized = materialize(rows);
        String orderedSha256;
        try {
            MessageDigest orderedDigest = MessageDigest.getInstance("SHA-256");
            for (RecordedRow row : rows) {
                update(orderedDigest, row.bytes);
            }
            orderedSha256 = hex(orderedDigest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
        rows.sort((left, right) -> Arrays.compareUnsigned(left.bytes, right.bytes));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (RecordedRow row : rows) {
                update(digest, row.bytes);
            }
            List<String> debugRows = new ArrayList<>(rows.size());
            for (RecordedRow row : rows) {
                debugRows.add(row.debug);
            }
            debugRows.sort(String::compareTo);
            MessageDigest materializedDigest = MessageDigest.getInstance("SHA-256");
            List<String> materializedDebugRows = new ArrayList<>();
            long materializedCount = 0;
            for (Map.Entry<byte[], MaterializedRow> entry : materialized.entrySet()) {
                if (entry.getValue().count < 0) {
                    throw new IllegalStateException("Benchmark changelog retracts a row that is not present");
                }
                for (int occurrence = 0; occurrence < entry.getValue().count; occurrence++) {
                    byte[] bytes = entry.getValue().bytes;
                    materializedDigest.update((byte) (bytes.length >>> 24));
                    materializedDigest.update((byte) (bytes.length >>> 16));
                    materializedDigest.update((byte) (bytes.length >>> 8));
                    materializedDigest.update((byte) bytes.length);
                    materializedDigest.update(bytes);
                    materializedDebugRows.add(entry.getValue().debug);
                    materializedCount++;
                }
            }
            return new Result(
                    rows.size(),
                    hex(digest.digest()),
                    orderedSha256,
                    debugRows,
                    materializedCount,
                    hex(materializedDigest.digest()),
                    materializedDebugRows);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static TreeMap<byte[], MaterializedRow> materialize(List<RecordedRow> rows) {
        TreeMap<byte[], MaterializedRow> materialized = new TreeMap<>(Arrays::compareUnsigned);
        for (RecordedRow row : rows) {
            if (row.materializationKey != null) {
                if (row.accumulate) {
                    materialized.put(
                            row.materializationKey,
                            new MaterializedRow(row.materializedBytes, row.materializedDebug, 1));
                } else if (materialized.remove(row.materializationKey) == null) {
                    throw new IllegalStateException("Benchmark changelog retracts a key that is not present");
                }
                continue;
            }
            MaterializedRow value = materialized.computeIfAbsent(
                    row.materializedBytes,
                    ignored -> new MaterializedRow(row.materializedBytes, row.materializedDebug, 0));
            value.count += row.accumulate ? 1 : -1;
            if (value.count == 0) {
                materialized.remove(row.materializedBytes);
            }
        }
        return materialized;
    }

    private static String hex(byte[] bytes) {
        char[] output = new char[bytes.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            output[index * 2] = digits[value >>> 4];
            output[index * 2 + 1] = digits[value & 0x0f];
        }
        return new String(output);
    }

    private static void update(MessageDigest digest, byte[] bytes) {
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    static final class Result {
        private final long rowCount;
        private final String sha256;
        private final String orderedSha256;
        private final List<String> debugRows;
        private final long materializedRowCount;
        private final String materializedSha256;
        private final List<String> materializedDebugRows;

        Result(
                long rowCount,
                String sha256,
                String orderedSha256,
                List<String> debugRows,
                long materializedRowCount,
                String materializedSha256,
                List<String> materializedDebugRows) {
            this.rowCount = rowCount;
            this.sha256 = sha256;
            this.orderedSha256 = orderedSha256;
            this.debugRows = List.copyOf(debugRows);
            this.materializedRowCount = materializedRowCount;
            this.materializedSha256 = materializedSha256;
            this.materializedDebugRows = List.copyOf(materializedDebugRows);
        }

        long rowCount() {
            return rowCount;
        }

        String sha256() {
            return sha256;
        }

        String orderedSha256() {
            return orderedSha256;
        }

        List<String> debugRows() {
            return debugRows;
        }

        long materializedRowCount() {
            return materializedRowCount;
        }

        String materializedSha256() {
            return materializedSha256;
        }

        List<String> materializedDebugRows() {
            return materializedDebugRows;
        }
    }

    private static final class RecordedRow {
        private final byte[] bytes;
        private final byte[] materializedBytes;
        private final byte[] materializationKey;
        private final boolean accumulate;
        private final String debug;
        private final String materializedDebug;

        private RecordedRow(
                byte[] bytes,
                byte[] materializedBytes,
                byte[] materializationKey,
                boolean accumulate,
                String debug,
                String materializedDebug) {
            this.bytes = bytes;
            this.materializedBytes = materializedBytes;
            this.materializationKey = materializationKey;
            this.accumulate = accumulate;
            this.debug = debug;
            this.materializedDebug = materializedDebug;
        }
    }

    private static final class MaterializedRow {
        private final byte[] bytes;
        private final String debug;
        private int count;

        private MaterializedRow(byte[] bytes, String debug, int count) {
            this.bytes = bytes;
            this.debug = debug;
            this.count = count;
        }
    }
}
