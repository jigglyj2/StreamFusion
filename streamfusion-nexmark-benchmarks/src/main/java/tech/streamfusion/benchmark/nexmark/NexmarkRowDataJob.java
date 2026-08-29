package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Runs Nexmark's RowData generator through SQL into Flink's RowData blackhole sink. */
public final class NexmarkRowDataJob {
    public static final int PARALLELISM = 4;
    public static final long CHECKPOINT_INTERVAL_MILLIS = 1_000;

    private NexmarkRowDataJob() {}

    public static void run(long eventCount, String query, boolean streamFusion) throws Exception {
        if (eventCount <= 0) {
            throw new IllegalArgumentException("eventCount must be positive");
        }
        configurePlanner(streamFusion);
        TableEnvironment tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, "hashmap");
        tables.getConfig()
                .getConfiguration()
                .setString("execution.checkpointing.interval", CHECKPOINT_INTERVAL_MILLIS + " ms");
        tables.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        tables.getConfig().getConfiguration().setString("execution.checkpointing.max-concurrent-checkpoints", "1");
        tables.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, PARALLELISM);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, PARALLELISM);

        tables.executeSql(sourceDdl(eventCount));
        NexmarkSqlJob.createViews(tables);
        tables.executeSql(sinkDdl(query));
        tables.executeSql("INSERT INTO nexmark_output\n" + NexmarkRowDataQueryCatalog.load(query))
                .await();
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
                + "','first-event.rate'='2147483647','next-event.rate'='2147483647',"
                + "'max-emit-speed'='true','keep-alive'='false')";
    }

    static String sinkDdl(String query) throws IOException {
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
            default:
                throw new IOException("Nexmark query is not fully accelerable: " + query);
        }
        return "CREATE TABLE nexmark_output (" + columns + ") WITH ('connector'='blackhole')";
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
}
