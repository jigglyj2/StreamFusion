// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use datafusion::error::{DataFusionError, Result};

#[derive(Debug, PartialEq)]
pub(super) enum Part {
    YearOfEra,
    Format(String),
}

/// Translate the locale-independent numeric subset of Java DateTimeFormatter patterns.
/// Calendar and formatting kernels consume the resulting chrono format; yyyy is adapted
/// separately because Java uses year-of-era and EXCEEDS_PAD sign rules.
pub(super) fn parse(pattern: &str) -> Result<Vec<Part>> {
    let mut parts = Vec::new();
    let mut format = String::new();
    let mut chars = pattern.chars().peekable();
    let mut quoted = false;
    let invalid = || {
        DataFusionError::Plan("DATE_FORMAT requires literal numeric fields yyyy, MM, dd, HH, mm, ss, SSS and quoted literals; other Java patterns stay on Flink".into())
    };
    while let Some(c) = chars.next() {
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                format.push('\'');
            } else {
                quoted = !quoted;
            }
        } else if !quoted && c.is_ascii_alphabetic() {
            let mut count = 1;
            while chars.peek() == Some(&c) {
                chars.next();
                count += 1;
            }
            let token = match (c, count) {
                ('y', 4) => {
                    if !format.is_empty() {
                        parts.push(Part::Format(std::mem::take(&mut format)));
                    }
                    parts.push(Part::YearOfEra);
                    continue;
                }
                ('M', 2) => "%m",
                ('d', 2) => "%d",
                ('H', 2) => "%H",
                ('m', 2) => "%M",
                ('s', 2) => "%S",
                ('S', 3) => "%3f",
                _ => return Err(invalid()),
            };
            format.push_str(token);
        } else {
            if !quoted && matches!(c, '[' | ']' | '{' | '}' | '#') {
                return Err(invalid());
            }
            if c == '%' {
                format.push('%');
            }
            format.push(c);
        }
    }
    if quoted {
        return Err(invalid());
    }
    if !format.is_empty() || parts.is_empty() {
        parts.push(Part::Format(format));
    }
    Ok(parts)
}
