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

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class Sha2DynamicParityTest extends SqlParityTestSupport {
    @Test
    void nativeDynamicSha2MatchesFlinkPerRowSelectionAndNulls() throws Exception {
        assertDataStreamParity(
                "SELECT SHA2(metric, CASE"
                        + " WHEN metric = 'sha224' THEN 224"
                        + " WHEN metric = 'sha256' THEN 256"
                        + " WHEN metric = 'sha384' THEN 384"
                        + " WHEN metric = 'null-length' THEN CAST(NULL AS INT)"
                        + " ELSE 512 END) FROM sha2_dynamic_input",
                Types.STRING,
                Arrays.asList(
                        Row.of("sha224"),
                        Row.of("sha256"),
                        Row.of("sha384"),
                        Row.of("sha512"),
                        Row.of("null-length"),
                        Row.of("你好😀"),
                        Row.of((Object) null)),
                "sha2_dynamic_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}
