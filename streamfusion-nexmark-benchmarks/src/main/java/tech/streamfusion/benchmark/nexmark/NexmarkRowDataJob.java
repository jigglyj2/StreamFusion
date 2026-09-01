package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Runs Nexmark's RowData generator through SQL into a deterministic changelog result sink. */
public final class NexmarkRowDataJob {
    public static final int PARALLELISM = 4;
    public static final long CHECKPOINT_INTERVAL_MILLIS = 1_000;
    public static final long MANAGED_MEMORY_MEBIBYTES = 1024;

    private NexmarkRowDataJob() {}

    public static BenchmarkResultStore.Result run(long eventCount, String query, boolean streamFusion)
            throws Exception {
        return run(eventCount, query, streamFusion, "hashmap");
    }

    public static BenchmarkResultStore.Result run(
            long eventCount, String query, boolean streamFusion, String stateBackend) throws Exception {
        return run(eventCount, query, streamFusion, stateBackend, PARALLELISM);
    }

    static BenchmarkResultStore.Result run(
            long eventCount, String query, boolean streamFusion, String stateBackend, int parallelism)
            throws Exception {
        if (eventCount <= 0) {
            throw new IllegalArgumentException("eventCount must be positive");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        if (!stateBackend.equals("hashmap") && !stateBackend.equals("rocksdb")) {
            throw new IllegalArgumentException("stateBackend must be hashmap or rocksdb: " + stateBackend);
        }
        configurePlanner(streamFusion);
        String resultRunId = java.util.UUID.randomUUID().toString();
        BenchmarkResultStore.begin(resultRunId);
        Path checkpointDirectory = Files.createTempDirectory("streamfusion-nexmark-checkpoints-");
        boolean completed = false;
        try {
            TableEnvironment tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            // Keep the local comparison out of Flink's tiny embedded-cluster defaults. Both
            // engines receive the same realistic state/Arrow allowance, including RocksDB cache
            // memory.
            tables.getConfig()
                    .getConfiguration()
                    .set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(MANAGED_MEMORY_MEBIBYTES));
            tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, stateBackend);
            tables.getConfig().getConfiguration().set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            tables.getConfig()
                    .getConfiguration()
                    .set(
                            CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                            checkpointDirectory.toUri().toString());
            tables.getConfig()
                    .getConfiguration()
                    .setString("execution.checkpointing.interval", CHECKPOINT_INTERVAL_MILLIS + " ms");
            tables.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");
            tables.getConfig().getConfiguration().setString("execution.checkpointing.max-concurrent-checkpoints", "1");
            tables.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);

            tables.executeSql(sourceDdl(eventCount));
            NexmarkSqlJob.createViews(tables);
            tables.executeSql(sinkDdl(query, resultRunId));
            tables.executeSql("INSERT INTO nexmark_output\n" + NexmarkRowDataQueryCatalog.load(query))
                    .await();
            completed = true;
            return BenchmarkResultStore.finish(resultRunId);
        } finally {
            if (!completed) {
                BenchmarkResultStore.finish(resultRunId);
            }
            deleteDirectory(checkpointDirectory);
        }
    }

    static String sourceDdl(long eventCount) {
        return "CREATE TABLE nexmark_events (event_type INT, "
                + "person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING, state STRING, `dateTime` TIMESTAMP(3), extra STRING>, "
                + "auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT, reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra STRING>, "
                + "bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `dateTime` TIMESTAMP(3), extra STRING>, "
                + "event_time AS CASE WHEN event_type=0 THEN person.`dateTime` WHEN event_type=1 THEN auction.`dateTime` ELSE bid.`dateTime` END, "
                + "WATERMARK FOR event_time AS event_time - INTERVAL '4' SECOND) WITH ("
                + "'connector'='streamfusion-nexmark-bounded','events.num'='"
                + eventCount
                + "','events.start-time'='1600000000000','first-event.rate'='2147483647','next-event.rate'='2147483647',"
                + "'max-emit-speed'='true','keep-alive'='false')";
    }

    static String sinkDdl(String query) throws IOException {
        return sinkDdl(query, "test-run");
    }

    static String sinkDdl(String query, String runId) throws IOException {
        String columns;
        switch (query) {
            case "q0":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), extra STRING";
                break;
            case "q1":
                columns = "auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` TIMESTAMP(3), extra STRING";
                break;
            case "q2":
                columns = "auction BIGINT, price BIGINT";
                break;
            case "q22":
                columns =
                        "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, dir1 STRING, dir2 STRING, dir3 STRING";
                break;
            case "group-aggregate":
                columns = "bidder BIGINT, bids BIGINT, spend BIGINT, minimum_price BIGINT, maximum_price BIGINT";
                break;
            case "select-distinct":
                columns = "bidder BIGINT";
                break;
            case "q11":
            case "q12":
                columns = "bidder BIGINT, bid_count BIGINT, starttime TIMESTAMP(3), endtime TIMESTAMP(3)";
                break;
            case "q8":
                columns = "id BIGINT, name STRING, starttime TIMESTAMP(3)";
                break;
            default:
                throw new IOException("Nexmark query is not fully accelerable: " + query);
        }
        return "CREATE TABLE nexmark_output ("
                + columns
                + ") WITH ('connector'='streamfusion-benchmark-result','run-id'='"
                + runId
                + "')";
    }

    private static void configurePlanner(boolean streamFusion) {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException failure) throws IOException {
                    if (failure instanceof NoSuchFileException) {
                        return FileVisitResult.CONTINUE;
                    }
                    throw failure;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException failure) throws IOException {
                    if (failure != null && !(failure instanceof NoSuchFileException)) {
                        throw failure;
                    }
                    Files.deleteIfExists(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (NoSuchFileException ignored) {
            // Flink may finish asynchronous checkpoint cleanup before this best-effort walk.
        }
    }
}
