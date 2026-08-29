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
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class DynamicStartsWithParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('', ''), ('abc', 'ab'), ('abc', ''), ('abc', 'bc'), "
            + "('😀stream', '😀'), ('你好世界', '你好'), "
            + "(CAST(NULL AS STRING), 'a'), ('a', CAST(NULL AS STRING)), "
            + "(CAST(NULL AS STRING), CAST(NULL AS STRING))) input(text_value, prefix)";

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SELECT text_value, prefix, STARTSWITH(text_value, prefix) FROM " + INPUT,
                "SELECT text_value, prefix FROM " + INPUT + " WHERE STARTSWITH(text_value, prefix)"
            })
    void dynamicPrefixMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}
