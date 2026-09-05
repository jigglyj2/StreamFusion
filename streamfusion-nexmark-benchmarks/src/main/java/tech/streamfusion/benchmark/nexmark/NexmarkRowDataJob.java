package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
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
                    .set(
                            TaskManagerOptions.MANAGED_MEMORY_SIZE,
                            MemorySize.ofMebiBytes(
                                    Long.getLong("streamfusion.nexmark.managed-memory-mb", MANAGED_MEMORY_MEBIBYTES)));
            // These Arrow-heavy jobs need more scratch/output memory than Flink's default 50/50
            // split, while their one-million-event RocksDB working set needs far less than half a
            // GiB of cache. This is Flink's standard consumer-weight setting and is identical for
            // both engines.
            tables.getConfig()
                    .getConfiguration()
                    .setString(
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS.key(),
                            System.getProperty(
                                    TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS.key(),
                                    "OPERATOR:90,STATE_BACKEND:10,PYTHON:30"));
            tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, stateBackend);
            tables.getConfig().getConfiguration().set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            tables.getConfig()
                    .getConfiguration()
                    .set(
                            CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                            checkpointDirectory.toUri().toString());
            // Flink's bounded full-sort transformation declares sorted inputs, and Flink's
            // OneInputStreamTask rejects checkpointing for that input contract. Leave periodic
            // checkpoints disabled for both engines in this one apples-to-apples workload. The
            // native operator's aligned/unaligned and cross-backend recovery is exercised by its
            // dedicated operator tests.
            if (!query.equals("bounded-sort")) {
                tables.getConfig()
                        .getConfiguration()
                        .setString(
                                "execution.checkpointing.interval",
                                Long.getLong("streamfusion.nexmark.checkpoint-interval-ms", CHECKPOINT_INTERVAL_MILLIS)
                                        + " ms");
                tables.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");
                tables.getConfig()
                        .getConfiguration()
                        .setString("execution.checkpointing.max-concurrent-checkpoints", "1");
            }
            tables.getConfig().getConfiguration().setString("restart-strategy.type", "none");
            tables.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            boolean miniBatch = Boolean.getBoolean("streamfusion.nexmark.mini-batch");
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, miniBatch);
            if (miniBatch) {
                tables.getConfig()
                        .set(
                                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE,
                                Long.getLong("streamfusion.nexmark.mini-batch-size", 5_000L));
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofDays(1));
            }
            String aggregatePhase = System.getProperty("streamfusion.nexmark.aggregate-phase", "AUTO");
            tables.getConfig()
                    .set(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                            AggregatePhaseStrategy.valueOf(aggregatePhase.toUpperCase(java.util.Locale.ROOT)));
            if (query.equals("incremental-group-aggregate")) {
                tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
            }
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
            tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
            if (query.equals("bounded-sort")) {
                tables.getConfig().getConfiguration().setString("__table.exec.sort.non-temporal.enabled__", "true");
            }

            tables.executeSql(sourceDdl(eventCount));
            NexmarkSqlJob.createViews(tables);
            if (query.equals("temporal-join")) {
                tables.executeSql(versionedAuctionViewDdl());
            }
            tables.executeSql(sinkDdl(query, resultRunId));
            String statement = "INSERT INTO nexmark_output\n" + NexmarkRowDataQueryCatalog.load(query);
            if (Boolean.getBoolean("streamfusion.nexmark.debug-plan")) {
                System.out.println(tables.explainSql(statement));
            }
            tables.executeSql(statement).await();
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

    private static String versionedAuctionViewDdl() {
        return "CREATE VIEW versioned_auction AS "
                + "SELECT id, seller, category, `dateTime` FROM ("
                + "SELECT id, seller, category, `dateTime`, "
                + "ROW_NUMBER() OVER (PARTITION BY id ORDER BY `dateTime` DESC) AS row_num "
                + "FROM auction) WHERE row_num = 1";
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
            case "q18":
            case "deduplicate-processing-time-keep-first":
            case "deduplicate-processing-time-keep-last":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, "
                        + "`dateTime` TIMESTAMP(3), extra STRING";
                break;
            case "q3":
                columns = "name STRING, city STRING, state STRING, id BIGINT";
                break;
            case "q4":
                columns = "category BIGINT, final BIGINT, PRIMARY KEY (category) NOT ENFORCED";
                break;
            case "q9":
                columns = "id BIGINT, itemName STRING, description STRING, initialBid BIGINT, reserve BIGINT, "
                        + "`dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, "
                        + "auction_extra STRING, auction BIGINT, bidder BIGINT, price BIGINT, "
                        + "bid_dateTime TIMESTAMP(3), bid_extra STRING, PRIMARY KEY (id) NOT ENFORCED";
                break;
            case "interval-join":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, bid_time TIMESTAMP(3)";
                break;
            case "temporal-join":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, bid_time TIMESTAMP(3), "
                        + "seller BIGINT, category BIGINT";
                break;
            case "q19":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, "
                        + "`dateTime` TIMESTAMP(3), extra STRING, rank_number BIGINT";
                break;
            case "q20":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, "
                        + "bid_dateTime TIMESTAMP(3), bid_extra STRING, itemName STRING, description STRING, "
                        + "initialBid BIGINT, reserve BIGINT, auction_dateTime TIMESTAMP(3), expires TIMESTAMP(3), "
                        + "seller BIGINT, category BIGINT, auction_extra STRING";
                break;
            case "q22":
                columns =
                        "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, dir1 STRING, dir2 STRING, dir3 STRING";
                break;
            case "q23":
                columns = "bidder BIGINT, price BIGINT, channel STRING, url STRING, bid_extra STRING, "
                        + "person_id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING, "
                        + "state STRING, person_extra STRING, itemName STRING, description STRING, "
                        + "initialBid BIGINT, reserve BIGINT, auction_dateTime TIMESTAMP(3), expires TIMESTAMP(3), "
                        + "seller BIGINT, category BIGINT, auction_extra STRING";
                break;
            case "group-aggregate":
                columns = "bidder BIGINT, bids BIGINT, spend BIGINT, minimum_price BIGINT, maximum_price BIGINT";
                break;
            case "aggregate-modifiers":
            case "incremental-group-aggregate":
                columns = "bidder BIGINT, distinct_auctions BIGINT, expensive_bids BIGINT, "
                        + "distinct_expensive_spend BIGINT, average_price BIGINT, "
                        + "average_decimal_price DECIMAL(38, 6), "
                        + "average_distinct_expensive_price BIGINT, minimum_expensive_price BIGINT, "
                        + "maximum_expensive_price BIGINT";
                break;
            case "global-aggregate":
                columns = "bids BIGINT, prices BIGINT, spend BIGINT, minimum_bidder BIGINT, maximum_auction BIGINT";
                break;
            case "grouping-sets":
                columns = "bidder BIGINT, channel STRING, bids BIGINT, spend BIGINT";
                break;
            case "legacy-window-aggregate":
                columns = "bidder BIGINT, bid_count BIGINT, spend BIGINT, average_price BIGINT, "
                        + "minimum_price BIGINT, maximum_price BIGINT, starttime TIMESTAMP(3), endtime TIMESTAMP(3)";
                break;
            case "match-recognize":
                columns = "bidder BIGINT, first_auction BIGINT, second_auction BIGINT, third_auction BIGINT";
                break;
            case "over-aggregate":
            case "over-aggregate-event-time":
            case "over-aggregate-processing-time":
            case "over-aggregate-bounded-rows":
            case "over-aggregate-bounded-range":
                columns = "bidder BIGINT, auction BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), running_spend BIGINT";
                break;
            case "select-distinct":
                columns = "bidder BIGINT";
                break;
            case "set-intersect-all":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT";
                break;
            case "top-n":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), "
                        + "extra STRING, row_num BIGINT";
                break;
            case "limit":
            case "bounded-sort":
            case "temporal-sort":
                columns = "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), extra STRING";
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
