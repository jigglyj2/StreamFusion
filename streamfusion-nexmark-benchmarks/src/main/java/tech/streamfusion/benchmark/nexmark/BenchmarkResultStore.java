package tech.streamfusion.benchmark.nexmark;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    static void add(String runId, byte[] row, String debug) {
        ConcurrentLinkedQueue<RecordedRow> rows = RUNS.get(runId);
        if (rows == null) {
            throw new IllegalStateException("Unknown benchmark result run: " + runId);
        }
        rows.add(new RecordedRow(row, debug));
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
            return new Result(rows.size(), hex(digest.digest()), debugRows);
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

        Result(long rowCount, String sha256, List<String> debugRows) {
            this.rowCount = rowCount;
            this.sha256 = sha256;
            this.debugRows = List.copyOf(debugRows);
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
    }

    private static final class RecordedRow {
        private final byte[] bytes;
        private final String debug;

        private RecordedRow(byte[] bytes, String debug) {
            this.bytes = bytes;
            this.debug = debug;
        }
    }
}
