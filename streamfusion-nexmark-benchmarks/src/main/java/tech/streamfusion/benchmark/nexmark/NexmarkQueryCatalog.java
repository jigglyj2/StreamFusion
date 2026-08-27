package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class NexmarkQueryCatalog {
    private static final List<String> SUPPORTED_QUERIES = List.of(
            "q0", "q1", "q2", "q3", "q4", "q5", "q7", "q8", "q9", "q10", "q11", "q12", "q13", "q14", "q15", "q16",
            "q17", "q18", "q19", "q20", "q21", "q22", "q23");

    private NexmarkQueryCatalog() {}

    public static List<String> supportedQueries() {
        return SUPPORTED_QUERIES;
    }

    public static String load(String query) throws IOException {
        if (!SUPPORTED_QUERIES.contains(query)) {
            throw new IOException("Unsupported Nexmark query: " + query);
        }
        try (InputStream input = NexmarkQueryCatalog.class.getResourceAsStream("/nexmark/queries/" + query + ".sql")) {
            if (input == null) {
                throw new IOException("Missing Nexmark query resource: " + query);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
