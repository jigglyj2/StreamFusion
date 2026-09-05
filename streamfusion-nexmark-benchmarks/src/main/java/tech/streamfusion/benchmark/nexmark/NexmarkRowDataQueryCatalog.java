package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Canonical SQL-resource and result-schema catalog for fully accelerable RowData Nexmark jobs. */
final class NexmarkRowDataQueryCatalog {
    private static final String BID_COLUMNS =
            "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), extra STRING";
    private static final String FULL_BID_COLUMNS = "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, "
            + "url STRING, `dateTime` TIMESTAMP(3), extra STRING";
    private static final String OVER_COLUMNS =
            "bidder BIGINT, auction BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), running_spend BIGINT";
    private static final String SESSION_COLUMNS =
            "bidder BIGINT, bid_count BIGINT, starttime TIMESTAMP(3), endtime TIMESTAMP(3)";
    private static final Map<String, String> SINK_COLUMNS = createSinkColumns();

    private NexmarkRowDataQueryCatalog() {}

    static List<String> supportedQueries() {
        return List.copyOf(SINK_COLUMNS.keySet());
    }

    static String load(String query) throws IOException {
        requireSupported(query);
        String resource = "/nexmark/rowdata/" + query + ".sql";
        try (InputStream input = NexmarkRowDataQueryCatalog.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IOException("Missing RowData Nexmark query resource: " + query);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static String sinkColumns(String query) throws IOException {
        requireSupported(query);
        return SINK_COLUMNS.get(query);
    }

    private static void requireSupported(String query) throws IOException {
        if (!SINK_COLUMNS.containsKey(query)) {
            throw new IOException("Nexmark query is not fully accelerable: " + query);
        }
    }

    private static Map<String, String> createSinkColumns() {
        Map<String, String> schemas = new LinkedHashMap<>();
        schemas.put("q0", BID_COLUMNS);
        schemas.put("q1", "auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` TIMESTAMP(3), extra STRING");
        schemas.put("q2", "auction BIGINT, price BIGINT");
        schemas.put("q3", "name STRING, city STRING, state STRING, id BIGINT");
        schemas.put("q4", "category BIGINT, final BIGINT, PRIMARY KEY (category) NOT ENFORCED");
        schemas.put("q5", "auction BIGINT, num BIGINT");
        schemas.put("q7", BID_COLUMNS);
        schemas.put("q8", "id BIGINT, name STRING, starttime TIMESTAMP(3)");
        schemas.put(
                "q9",
                "id BIGINT, itemName STRING, description STRING, initialBid BIGINT, reserve BIGINT, "
                        + "`dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, "
                        + "auction_extra STRING, auction BIGINT, bidder BIGINT, price BIGINT, "
                        + "bid_dateTime TIMESTAMP(3), bid_extra STRING, PRIMARY KEY (id) NOT ENFORCED");
        schemas.put("q11", SESSION_COLUMNS);
        schemas.put("q12", SESSION_COLUMNS);
        schemas.put("q18", FULL_BID_COLUMNS);
        schemas.put("q19", FULL_BID_COLUMNS + ", rank_number BIGINT");
        schemas.put(
                "q20",
                "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, "
                        + "bid_dateTime TIMESTAMP(3), bid_extra STRING, itemName STRING, description STRING, "
                        + "initialBid BIGINT, reserve BIGINT, auction_dateTime TIMESTAMP(3), expires TIMESTAMP(3), "
                        + "seller BIGINT, category BIGINT, auction_extra STRING");
        schemas.put(
                "q22",
                "auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, dir1 STRING, dir2 STRING, dir3 STRING");
        schemas.put(
                "q23",
                "bidder BIGINT, price BIGINT, channel STRING, url STRING, bid_extra STRING, "
                        + "person_id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING, "
                        + "state STRING, person_extra STRING, itemName STRING, description STRING, "
                        + "initialBid BIGINT, reserve BIGINT, auction_dateTime TIMESTAMP(3), expires TIMESTAMP(3), "
                        + "seller BIGINT, category BIGINT, auction_extra STRING");
        schemas.put(
                "aggregate-modifiers",
                "bidder BIGINT, distinct_auctions BIGINT, expensive_bids BIGINT, "
                        + "distinct_expensive_spend BIGINT, average_price BIGINT, "
                        + "average_decimal_price DECIMAL(38, 6), "
                        + "average_distinct_expensive_price BIGINT, minimum_expensive_price BIGINT, "
                        + "maximum_expensive_price BIGINT");
        schemas.put("batch-unnest", "auction BIGINT, expanded_value BIGINT");
        schemas.put(
                "batch-window-tvf",
                "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), "
                        + "window_start TIMESTAMP(3), window_end TIMESTAMP(3), window_time TIMESTAMP(3)");
        schemas.put("bounded-sort", BID_COLUMNS);
        schemas.put("bounded-sort-limit", BID_COLUMNS);
        schemas.put("bounded-limit", BID_COLUMNS);
        schemas.put("bounded-rank", BID_COLUMNS + ", rank_number BIGINT");
        schemas.put("incremental-group-aggregate", schemas.get("aggregate-modifiers"));
        schemas.put(
                "group-aggregate",
                "bidder BIGINT, bids BIGINT, spend BIGINT, minimum_price BIGINT, maximum_price BIGINT");
        schemas.put(
                "global-aggregate",
                "bids BIGINT, prices BIGINT, spend BIGINT, minimum_bidder BIGINT, maximum_auction BIGINT");
        schemas.put("grouping-sets", "bidder BIGINT, channel STRING, bids BIGINT, spend BIGINT");
        schemas.put(
                "legacy-window-aggregate",
                "bidder BIGINT, bid_count BIGINT, spend BIGINT, average_price BIGINT, "
                        + "minimum_price BIGINT, maximum_price BIGINT, starttime TIMESTAMP(3), endtime TIMESTAMP(3)");
        schemas.put(
                "match-recognize", "bidder BIGINT, first_auction BIGINT, second_auction BIGINT, third_auction BIGINT");
        schemas.put("interval-join", "auction BIGINT, bidder BIGINT, price BIGINT, bid_time TIMESTAMP(3)");
        schemas.put(
                "temporal-join",
                "auction BIGINT, bidder BIGINT, price BIGINT, bid_time TIMESTAMP(3), seller BIGINT, category BIGINT");
        schemas.put("over-aggregate", OVER_COLUMNS);
        schemas.put("over-aggregate-event-time", OVER_COLUMNS);
        schemas.put("over-aggregate-processing-time", OVER_COLUMNS);
        schemas.put("over-aggregate-bounded-rows", OVER_COLUMNS);
        schemas.put("over-aggregate-bounded-range", OVER_COLUMNS);
        schemas.put("select-distinct", "bidder BIGINT");
        schemas.put("set-intersect-all", "auction BIGINT, bidder BIGINT, price BIGINT");
        schemas.put("deduplicate-processing-time-keep-first", FULL_BID_COLUMNS);
        schemas.put("deduplicate-processing-time-keep-last", FULL_BID_COLUMNS);
        schemas.put("temporal-sort", BID_COLUMNS);
        schemas.put("top-n", BID_COLUMNS + ", row_num BIGINT");
        schemas.put("limit", BID_COLUMNS);
        return Collections.unmodifiableMap(schemas);
    }
}
