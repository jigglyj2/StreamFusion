/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package com.github.nexmark.flink.source;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.github.nexmark.flink.model.Event;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.TimestampData;

/** Legal last-bid results across reader interleavings, preserving each reader's event order. */
public final class NexmarkLastBidOracle {
    private NexmarkLastBidOracle() {}

    public static Map<List<Long>, Set<String>> candidates(long events, int parallelism) throws Exception {
        var options = new Configuration();
        options.setString("events.num", Long.toString(events));
        options.setString("first-event.rate", "2147483647");
        options.setString("next-event.rate", "2147483647");
        options.setString("max-emit-speed", "true");
        options.setString("keep-alive", "false");
        var config = NexmarkSourceOptions.convertToNexmarkConfiguration(options);
        config.numEventGenerators = parallelism;
        var generatorConfig = new GeneratorConfig(config, 1600000000000L, 1, events, config.stopAtEvent, 1);
        var seed = StreamFusionDeterministicNexmarkSourceReader.class.getDeclaredField("RANDOM_SEED");
        seed.setAccessible(true);
        var setRandom = StreamFusionDeterministicNexmarkSourceReader.class.getDeclaredMethod(
                "setRandom", NexmarkGenerator.class, long.class);
        setRandom.setAccessible(true);
        var latest = new HashMap<List<Long>, Long>();
        var candidates = new HashMap<List<Long>, Set<String>>();
        for (var split : generatorConfig.split(parallelism)) {
            var readerConfig = split.reconfigure(generatorConfig, generatorConfig.isSourceIgnoreStop());
            var generator = new NexmarkGenerator(readerConfig, 0, -1);
            setRandom.invoke(null, generator, seed.getLong(null) ^ split.getStartEventId());
            var rows = new HashMap<List<Long>, String>();
            var times = new HashMap<List<Long>, Long>();
            var deserializer = new StreamFusionDeterministicRowDataEventDeserializer();
            while (generator.hasNext()) {
                var event = generator.next().event;
                // Exercise exactly the source's normalization, without modifying the generator.
                deserializer.deserialize(event);
                if (event.type != Event.Type.BID) continue;
                var bid = event.bid;
                var key = List.of(bid.auction, bid.bidder);
                long time = bid.dateTime.toEpochMilli();
                if (times.containsKey(key) && times.get(key) > time) continue;
                times.put(key, time);
                rows.put(
                        key,
                        "+I"
                                + Arrays.toString(new Object[] {
                                    bid.auction,
                                    bid.bidder,
                                    bid.price,
                                    bid.channel,
                                    bid.url,
                                    TimestampData.fromEpochMillis(time),
                                    bid.extra
                                }));
            }
            for (var entry : times.entrySet()) {
                var key = entry.getKey();
                long time = entry.getValue();
                if (!latest.containsKey(key) || time > latest.get(key)) {
                    latest.put(key, time);
                    candidates.put(key, new HashSet<>());
                }
                if (time == latest.get(key)) candidates.get(key).add(rows.get(key));
            }
        }
        return candidates;
    }
}
