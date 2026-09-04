/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.nexmark.flink.source;

import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;
import org.apache.flink.table.data.RowData;

/** Removes the upstream generator's process-random static string and URL caches. */
final class StreamFusionDeterministicRowDataEventDeserializer implements EventDeserializer<RowData> {
    private static final long GOLDEN_GAMMA = 0x9e37_79b9_7f4a_7c15L;
    private static final String URL_PREFIX = "https://www.nexmark.com/";
    private static final String URL_SUFFIX = "/item.htm?query=1";

    private final RowDataEventDeserializer delegate = new RowDataEventDeserializer();

    @Override
    public RowData deserialize(Event event) {
        switch (event.type) {
            case PERSON:
                normalizePerson(event.newPerson);
                break;
            case AUCTION:
                normalizeAuction(event.newAuction);
                break;
            case BID:
                normalizeBid(event.bid);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Nexmark event type: " + event.type);
        }
        return delegate.deserialize(event);
    }

    private static void normalizePerson(Person person) {
        person.extra = deterministicLetters(person.extra.length(), person.id ^ 0x7065_7273_6f6eL);
    }

    private static void normalizeAuction(Auction auction) {
        auction.extra = deterministicLetters(auction.extra.length(), auction.id ^ 0x6175_6374_696f_6eL);
    }

    private static void normalizeBid(Bid bid) {
        long seed = mix64(bid.auction)
                ^ Long.rotateLeft(mix64(bid.bidder), 17)
                ^ Long.rotateLeft(mix64(bid.price), 31)
                ^ bid.dateTime.toEpochMilli();
        bid.extra = deterministicLetters(bid.extra.length(), seed);
        bid.url = deterministicUrl(bid.channel);
    }

    private static String deterministicUrl(String channel) {
        long seed = mix64(channel.hashCode());
        String base = URL_PREFIX
                + deterministicLetters(5, seed)
                + '/'
                + deterministicLetters(5, seed + GOLDEN_GAMMA)
                + '/'
                + deterministicLetters(5, seed + 2 * GOLDEN_GAMMA)
                + URL_SUFFIX;
        if (!channel.startsWith("channel-")) {
            return base;
        }
        try {
            int channelId = Integer.parseInt(channel.substring("channel-".length()));
            // Retain the upstream cache's approximate 90/10 URL-shape distribution deterministically.
            if (Math.floorMod(mix64(channelId), 10) != 0) {
                return base + "&channel_id=" + Math.abs(Integer.reverse(channelId));
            }
        } catch (NumberFormatException ignored) {
            // A non-standard channel still gets a stable base URL.
        }
        return base;
    }

    private static String deterministicLetters(int length, long seed) {
        StringBuilder value = new StringBuilder(length);
        long state = seed;
        for (int index = 0; index < length; index++) {
            state += GOLDEN_GAMMA;
            value.append((char) ('a' + Math.floorMod(mix64(state), 26)));
        }
        return value.toString();
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58_476d_1ce4_e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d0_49bb_1331_11ebL;
        return value ^ (value >>> 31);
    }
}
