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
            boolean accumulate,
            String debug,
            String materializedDebug) {
        ConcurrentLinkedQueue<RecordedRow> rows = RUNS.get(runId);
        if (rows == null) {
            throw new IllegalStateException("Unknown benchmark result run: " + runId);
        }
        rows.add(new RecordedRow(changelogRow, materializedRow, accumulate, debug, materializedDebug));
    }

    static Result finish(String runId) {
        ConcurrentLinkedQueue<RecordedRow> collected = RUNS.remove(runId);
        if (collected == null) {
            throw new IllegalStateException("Unknown benchmark result run: " + runId);
        }
        List<RecordedRow> rows = new ArrayList<>(collected);
        rows.sort((left, right) -> Arrays.compareUnsigned(left.bytes, right.bytes));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (RecordedRow row : rows) {
                digest.update((byte) (row.bytes.length >>> 24));
                digest.update((byte) (row.bytes.length >>> 16));
                digest.update((byte) (row.bytes.length >>> 8));
                digest.update((byte) row.bytes.length);
                digest.update(row.bytes);
            }
            List<String> debugRows = new ArrayList<>(rows.size());
            for (RecordedRow row : rows) {
                debugRows.add(row.debug);
            }
            debugRows.sort(String::compareTo);
            TreeMap<byte[], MaterializedRow> materialized = new TreeMap<>(Arrays::compareUnsigned);
            for (RecordedRow row : rows) {
                MaterializedRow value = materialized.computeIfAbsent(
                        row.materializedBytes, ignored -> new MaterializedRow(row.materializedDebug));
                value.count += row.accumulate ? 1 : -1;
                if (value.count == 0) {
                    materialized.remove(row.materializedBytes);
                }
            }
            MessageDigest materializedDigest = MessageDigest.getInstance("SHA-256");
            List<String> materializedDebugRows = new ArrayList<>();
            long materializedCount = 0;
            for (Map.Entry<byte[], MaterializedRow> entry : materialized.entrySet()) {
                if (entry.getValue().count < 0) {
                    throw new IllegalStateException("Benchmark changelog retracts a row that is not present");
                }
                for (int occurrence = 0; occurrence < entry.getValue().count; occurrence++) {
                    byte[] bytes = entry.getKey();
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
                    debugRows,
                    materializedCount,
                    hex(materializedDigest.digest()),
                    materializedDebugRows);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
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

    static final class Result {
        private final long rowCount;
        private final String sha256;
        private final List<String> debugRows;
        private final long materializedRowCount;
        private final String materializedSha256;
        private final List<String> materializedDebugRows;

        Result(
                long rowCount,
                String sha256,
                List<String> debugRows,
                long materializedRowCount,
                String materializedSha256,
                List<String> materializedDebugRows) {
            this.rowCount = rowCount;
            this.sha256 = sha256;
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
        private final boolean accumulate;
        private final String debug;
        private final String materializedDebug;

        private RecordedRow(
                byte[] bytes, byte[] materializedBytes, boolean accumulate, String debug, String materializedDebug) {
            this.bytes = bytes;
            this.materializedBytes = materializedBytes;
            this.accumulate = accumulate;
            this.debug = debug;
            this.materializedDebug = materializedDebug;
        }
    }

    private static final class MaterializedRow {
        private final String debug;
        private int count;

        private MaterializedRow(String debug) {
            this.debug = debug;
        }
    }
}
