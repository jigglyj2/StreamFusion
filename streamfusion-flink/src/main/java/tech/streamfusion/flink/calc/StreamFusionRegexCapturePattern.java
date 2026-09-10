/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

/**
 * Planner-side syntax restriction and capture projection for DataFusion extraction.
 *
 * <p>Arrow regexp_match omits unmatched captures. Retaining exactly one capture avoids interpreting
 * that compacted list as Java's stable group numbering. The native decoder checks the projected
 * grammar again before composing DataFusion matching and element extraction.
 */
final class StreamFusionRegexCapturePattern {
    // Bound planner work and the prospective compiled expression independently of input size.
    private static final int MAX_PATTERN_LENGTH = 256;
    private static final int MAX_DEPTH = 32;

    private final String source;
    private final int selected;
    private int cursor;
    private int groups;

    private StreamFusionRegexCapturePattern(String source, int selected) {
        this.source = source;
        this.selected = selected;
    }

    /** Returns a pattern with one capture, or null when syntax/index is outside the verified grammar. */
    static String project(String pattern, int group) {
        if (pattern == null || pattern.length() > MAX_PATTERN_LENGTH || group < 0) return null;
        try {
            var parser = new StreamFusionRegexCapturePattern(pattern, group);
            String projected = parser.expression(0);
            if (parser.cursor != pattern.length() || group > parser.groups) return null;
            return group == 0 ? "(" + projected + ")" : projected;
        } catch (UnsupportedPattern ignored) {
            return null;
        }
    }

    private String expression(int depth) {
        if (depth > MAX_DEPTH) throw new UnsupportedPattern();
        var result = new StringBuilder();
        while (cursor < source.length() && source.charAt(cursor) != ')') {
            char next = source.charAt(cursor++);
            if (next == '(') {
                int currentGroup = ++groups;
                String inner = expression(depth + 1);
                if (cursor == source.length() || source.charAt(cursor++) != ')') throw new UnsupportedPattern();
                result.append(currentGroup == selected ? "(" : "(?:")
                        .append(inner)
                        .append(')');
                // Repeated groups retain stale captures in Java, unlike Rust's regex engine.
                if (cursor < source.length() && isQuantifier(source.charAt(cursor))) throw new UnsupportedPattern();
            } else if (next == '|' || next == '^') {
                result.append(next);
            } else {
                if (next == '[') {
                    result.append(characterClass());
                } else {
                    if (!isLiteral(next)) throw new UnsupportedPattern();
                    result.append(next);
                }
                if (cursor < source.length() && isQuantifier(source.charAt(cursor))) {
                    result.append(source.charAt(cursor++));
                }
            }
        }
        return result.toString();
    }

    private String characterClass() {
        int start = cursor - 1;
        if (cursor < source.length() && source.charAt(cursor) == '^') cursor++;
        int members = 0;
        while (cursor < source.length() && source.charAt(cursor) != ']') {
            char first = source.charAt(cursor++);
            if (!isClassLiteral(first)) throw new UnsupportedPattern();
            // Java intersections and Rust set operations are deliberately outside this grammar.
            if ((first == '&' || first == '|' || first == '~')
                    && cursor < source.length()
                    && source.charAt(cursor) == first) {
                throw new UnsupportedPattern();
            }
            if (cursor < source.length() && source.charAt(cursor) == '-') {
                cursor++;
                if (cursor == source.length()) throw new UnsupportedPattern();
                char last = source.charAt(cursor++);
                if (!isClassLiteral(last) || last < first) throw new UnsupportedPattern();
            }
            members++;
        }
        if (members == 0 || cursor == source.length()) throw new UnsupportedPattern();
        cursor++;
        return source.substring(start, cursor);
    }

    private static boolean isLiteral(char value) {
        return value >= ' ' && value <= '~' && "\\.^$|?*+()[]{}".indexOf(value) < 0;
    }

    private static boolean isClassLiteral(char value) {
        return value >= ' ' && value <= '~' && "\\[]^-".indexOf(value) < 0;
    }

    private static boolean isQuantifier(char value) {
        return value == '*' || value == '+' || value == '?';
    }

    private static final class UnsupportedPattern extends RuntimeException {
        private UnsupportedPattern() {
            super(null, null, false, false);
        }
    }
}
