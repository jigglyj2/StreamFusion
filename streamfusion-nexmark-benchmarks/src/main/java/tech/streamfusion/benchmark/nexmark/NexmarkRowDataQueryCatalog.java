package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Nexmark queries whose complete physical operator trees are currently accelerable. */
final class NexmarkRowDataQueryCatalog {
    private static final List<String> SUPPORTED = List.of(
            "q0",
            "q1",
            "q2",
            "q3",
            "q8",
            "q11",
            "q12",
            "q18",
            "q19",
            "q20",
            "q22",
            "q23",
            "aggregate-modifiers",
            "incremental-group-aggregate",
            "group-aggregate",
            "global-aggregate",
            "grouping-sets",
            "legacy-window-aggregate",
            "match-recognize",
            "interval-join",
            "temporal-join",
            "over-aggregate",
            "over-aggregate-event-time",
            "over-aggregate-processing-time",
            "over-aggregate-bounded-rows",
            "over-aggregate-bounded-range",
            "select-distinct",
            "deduplicate-processing-time-keep-first",
            "deduplicate-processing-time-keep-last",
            "temporal-sort",
            "top-n",
            "limit");

    private NexmarkRowDataQueryCatalog() {}

    static List<String> supportedQueries() {
        return SUPPORTED;
    }

    static String load(String query) throws IOException {
        if (!SUPPORTED.contains(query)) {
            throw new IOException("Nexmark query is not fully accelerable: " + query);
        }
        String resource = "/nexmark/rowdata/" + query + ".sql";
        try (InputStream input = NexmarkRowDataQueryCatalog.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IOException("Missing RowData Nexmark query resource: " + query);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
