package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

public final class NexmarkSqlJob {
    private NexmarkSqlJob() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "Usage: NexmarkSqlJob <bootstrap> <input-topic> <output-topic> <query> <flink|streamfusion>");
        }
        run(args[0], args[1], args[2], args[3], "streamfusion".equals(args[4]));
    }

    public static void run(String bootstrap, String inputTopic, String outputTopic, String query, boolean streamFusion)
            throws Exception {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        }
        TableEnvironment tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Configuration config = tables.getConfig().getConfiguration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.setString("execution.checkpointing.interval", "1s");
        config.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tables.executeSql(sourceDdl(bootstrap, inputTopic));
        createViews(tables);
        tables.executeSql(sinkDdl(bootstrap, outputTopic));
        tables.executeSql(loadQuery(query)).await();
    }

    static String loadQuery(String query) throws IOException {
        String resource = "/nexmark/" + query + ".sql";
        try (InputStream input = NexmarkSqlJob.class.getResourceAsStream(resource)) {
            if (input == null) throw new IOException("Unknown Nexmark query: " + query);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String sourceDdl(String bootstrap, String topic) {
        return "CREATE TABLE nexmark_events (event_type INT, "
                + "person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING, state STRING, `dateTime` TIMESTAMP(3), extra STRING>, "
                + "auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT, reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra STRING>, "
                + "bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `dateTime` TIMESTAMP(3), extra STRING>) WITH ("
                + "'connector'='kafka','topic'='" + topic + "','properties.bootstrap.servers'='" + bootstrap
                + "','properties.group.id'='streamfusion-nexmark','scan.startup.mode'='earliest-offset',"
                + "'scan.bounded.mode'='latest-offset','format'='json',"
                + "'json.timestamp-format.standard'='ISO-8601')";
    }

    static void createViews(TableEnvironment tables) {
        tables.executeSql(
                "CREATE VIEW person AS SELECT person.id, person.name, person.emailAddress, person.creditCard, person.city, person.state, person.`dateTime`, person.extra FROM nexmark_events WHERE event_type=0");
        tables.executeSql(
                "CREATE VIEW auction AS SELECT auction.id, auction.itemName, auction.description, auction.initialBid, auction.reserve, auction.`dateTime`, auction.expires, auction.seller, auction.category, auction.extra FROM nexmark_events WHERE event_type=1");
        tables.executeSql(
                "CREATE VIEW bid AS SELECT bid.auction, bid.bidder, bid.price, bid.channel, bid.url, bid.`dateTime`, bid.extra FROM nexmark_events WHERE event_type=2");
    }

    private static String sinkDdl(String bootstrap, String topic) {
        return "CREATE TABLE nexmark_output (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), extra STRING) WITH ("
                + "'connector'='kafka','topic'='" + topic + "','properties.bootstrap.servers'='" + bootstrap
                + "','format'='json','sink.delivery-guarantee'='exactly-once',"
                + "'properties.transaction.timeout.ms'='60000',"
                + "'sink.transactional-id-prefix'='streamfusion-nexmark-" + topic + "-')";
    }
}
