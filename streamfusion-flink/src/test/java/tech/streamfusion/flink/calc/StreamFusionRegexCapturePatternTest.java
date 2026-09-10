/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class StreamFusionRegexCapturePatternTest {
    @Test
    void projectsGroupNumbersWithoutDependingOnOtherGroupsParticipating() {
        assertThat(StreamFusionRegexCapturePattern.project("(a)|(b)", 2)).isEqualTo("(?:a)|(b)");
        assertThat(StreamFusionRegexCapturePattern.project("(&|^)channel_id=([^&]*)", 2))
                .isEqualTo("(?:&|^)channel_id=([^&]*)");
        assertThat(StreamFusionRegexCapturePattern.project("(a)(b)", 0)).isEqualTo("((?:a)(?:b))");
    }

    @Test
    void captureProjectionPreservesJavaFindForGeneratedUnicodeInputs() {
        var values = new ArrayList<>(List.of(
                "",
                "a",
                "ab",
                "b",
                "ababc",
                "channel_id=",
                "channel_id=hello&x=1",
                "x=1&channel_id=first&channel_id=second",
                "xchannel_id=wrong",
                "CHANNEL_ID=wrong",
                "channel_id=\n\r\u0085\u2028\u2029😀",
                "😀&channel_id=漢字é\u0301&",
                "\u0000a\u0000"));
        var random = new Random(21);
        String[] alphabet = {
            "a", "b", "c", "&", "=", "_", "1", "Z", " ", "\n", "\r", "\u0085", "\u2028", "\u2029", "\u0000", "😀", "漢",
            "é", "\u0301"
        };
        for (int row = 0; row < 512; row++) {
            var value = new StringBuilder();
            for (int i = 0, count = random.nextInt(50); i < count; i++)
                value.append(alphabet[random.nextInt(alphabet.length)]);
            values.add(value.toString());
            values.add("channel_id=" + value);
            values.add("prefix&channel_id=" + value + "&tail=1");
        }
        for (String original : List.of(
                "(&|^)channel_id=([^&]*)",
                "(a)|(b)",
                "((a)|b)(c*)",
                "(a*)a",
                "(a+)[bc]?",
                "([^a-c]+)",
                "(^|&)([a-zA-Z0-9_]*)",
                "a|(b)",
                "(a|ab)(b*)",
                "(a?)(a*)",
                "()",
                "")) {
            var pattern = Pattern.compile(original);
            int groups = pattern.matcher("").groupCount();
            for (int group = 0; group <= groups; group++) {
                String transformed = StreamFusionRegexCapturePattern.project(original, group);
                assertThat(transformed).as(original + " group " + group).isNotNull();
                var projected = Pattern.compile(transformed);
                assertThat(projected.matcher("").groupCount()).isEqualTo(1);
                for (String value : values) {
                    var before = pattern.matcher(value);
                    var after = projected.matcher(value);
                    String expected = before.find() ? before.group(group) : null;
                    String actual = after.find() ? after.group(1) : null;
                    assertThat(actual)
                            .as(original + " group " + group + " value " + value)
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    void rejectsSemanticAndResourceCasesOutsideTheGrammar() {
        for (String pattern : List.of(
                "a(?=b)",
                "(?<=a)b",
                "(?i)a",
                "(?:a)",
                "(?<name>a)",
                "(a)\\1",
                "\\d",
                "\\w",
                "\\b",
                "\\p{L}",
                "a$",
                ".",
                "[a-z&&[^b]]",
                "[a&&b]",
                "[a--b]",
                "[a~~b]",
                "[a||b]",
                "[a-z]++",
                "a*?",
                "a{2}",
                "(a)*",
                "((a)?b)+",
                "[😀]",
                "é",
                "[",
                "[z-a]",
                "[]",
                "[^]",
                "a)",
                "(a",
                "\\Qabc\\E",
                "a".repeat(257),
                "(".repeat(33) + "a" + ")".repeat(33))) {
            assertThat(StreamFusionRegexCapturePattern.project(pattern, 0))
                    .as(pattern)
                    .isNull();
        }
        assertThat(StreamFusionRegexCapturePattern.project(null, 0)).isNull();
        assertThat(StreamFusionRegexCapturePattern.project("(a)", -1)).isNull();
        assertThat(StreamFusionRegexCapturePattern.project("(a)", 2)).isNull();
    }
}
