package tech.streamfusion.benchmark.nexmark;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

final class KafkaBenchmarkTopics {
    private KafkaBenchmarkTopics() {}

    static void create(String bootstrapServers, String... topics) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        try (Admin admin = Admin.create(properties)) {
            List<NewTopic> definitions = java.util.Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .collect(java.util.stream.Collectors.toList());
            admin.createTopics(definitions).all().get(30, TimeUnit.SECONDS);
        }
    }

    static long countCommitted(String bootstrapServers, String topic, long expected, Duration timeout) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "streamfusion-counter-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        long count = 0;
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(topic));
            while (count < expected && System.nanoTime() < deadline) {
                count += consumer.poll(Duration.ofMillis(250)).count();
            }
        }
        return count;
    }
}
