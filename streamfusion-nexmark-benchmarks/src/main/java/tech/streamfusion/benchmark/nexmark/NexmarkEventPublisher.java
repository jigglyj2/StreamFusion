package tech.streamfusion.benchmark.nexmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

final class NexmarkEventPublisher {
    private static final ObjectMapper JSON = new ObjectMapper();

    private NexmarkEventPublisher() {}

    static long publish(String bootstrapServers, String topic, long eventCount) throws Exception {
        NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.copy();
        configuration.numEvents = eventCount;
        configuration.numEventGenerators = 1;
        configuration.isRateLimited = false;
        Generator generator = new Generator(new GeneratorConfig(configuration, 0, 0, eventCount, 0));

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        long bidCount = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (generator.hasNext()) {
                Event event = generator.next().getValue();
                if (event.bid != null) {
                    bidCount++;
                }
                producer.send(new ProducerRecord<>(topic, JSON.writeValueAsString(toKafkaEvent(event))));
            }
            producer.flush();
        }
        return bidCount;
    }

    private static Map<String, Object> toKafkaEvent(Event event) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (event.newPerson != null) {
            result.put("event_type", 0);
            Map<String, Object> person = new LinkedHashMap<>();
            person.put("id", event.newPerson.id);
            person.put("name", event.newPerson.name);
            person.put("emailAddress", event.newPerson.emailAddress);
            person.put("creditCard", event.newPerson.creditCard);
            person.put("city", event.newPerson.city);
            person.put("state", event.newPerson.state);
            person.put("dateTime", event.newPerson.dateTime.toString());
            person.put("extra", event.newPerson.extra);
            result.put("person", person);
        } else if (event.newAuction != null) {
            result.put("event_type", 1);
            Map<String, Object> auction = new LinkedHashMap<>();
            auction.put("id", event.newAuction.id);
            auction.put("itemName", event.newAuction.itemName);
            auction.put("description", event.newAuction.description);
            auction.put("initialBid", event.newAuction.initialBid);
            auction.put("reserve", event.newAuction.reserve);
            auction.put("dateTime", event.newAuction.dateTime.toString());
            auction.put("expires", event.newAuction.expires.toString());
            auction.put("seller", event.newAuction.seller);
            auction.put("category", event.newAuction.category);
            auction.put("extra", event.newAuction.extra);
            result.put("auction", auction);
        } else {
            result.put("event_type", 2);
            Map<String, Object> bid = new LinkedHashMap<>();
            bid.put("auction", event.bid.auction);
            bid.put("bidder", event.bid.bidder);
            bid.put("price", event.bid.price);
            bid.put("channel", "");
            bid.put("url", "");
            bid.put("dateTime", event.bid.dateTime.toString());
            bid.put("extra", event.bid.extra);
            result.put("bid", bid);
        }
        return result;
    }
}
