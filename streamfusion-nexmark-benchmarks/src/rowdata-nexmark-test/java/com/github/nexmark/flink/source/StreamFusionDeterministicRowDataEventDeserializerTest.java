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

import static org.assertj.core.api.Assertions.assertThat;

import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import java.time.Instant;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class StreamFusionDeterministicRowDataEventDeserializerTest {
    private final StreamFusionDeterministicRowDataEventDeserializer deserializer =
            new StreamFusionDeterministicRowDataEventDeserializer();

    @Test
    void canonicalizesProcessRandomBidPayloadWithoutShrinkingIt() {
        RowData left = deserialize("process-a-extra", "https://process-a.invalid/random");
        RowData right = deserialize("process-b-extra", "https://process-b.invalid/random");

        RowData leftBid = left.getRow(3, 7);
        RowData rightBid = right.getRow(3, 7);
        assertThat(leftBid.getString(4).toString())
                .isEqualTo(rightBid.getString(4).toString());
        assertThat(leftBid.getString(6).toString())
                .isEqualTo(rightBid.getString(6).toString());
        assertThat(leftBid.getString(6).toString()).hasSize("process-a-extra".length());
        assertThat(leftBid.getString(4).toString()).contains("/item.htm?query=1");
    }

    private RowData deserialize(String extra, String url) {
        return deserializer.deserialize(new Event(
                new Bid(1_001L, 2_002L, 3_003L, "channel-42", url, Instant.ofEpochMilli(1_600_000_000_000L), extra)));
    }
}
