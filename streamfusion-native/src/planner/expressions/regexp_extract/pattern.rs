// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use datafusion::error::{DataFusionError, Result};
use regex_syntax::ast::{AssertionKind, Ast, ClassSet, ClassSetItem, GroupKind, RepetitionKind};

pub(super) fn validate(pattern: &str) -> Result<()> {
    // Java permits 256 source bytes plus projection's noncapturing markers and outer group.
    if pattern.len() > 768
        || !pattern
            .bytes()
            .all(|b| (32..=126).contains(&b) && b != b'\\')
    {
        return Err(unsupported());
    }
    let ast = regex_syntax::ast::parse::ParserBuilder::new()
        .nest_limit(128)
        .build()
        .parse(pattern)
        .map_err(|_| unsupported())?;
    let mut captures = 0;
    if !supported(&ast, &mut captures) || captures != 1 {
        return Err(unsupported());
    }
    Ok(())
}
fn supported(ast: &Ast, captures: &mut usize) -> bool {
    match ast {
        Ast::Empty(_) | Ast::Literal(_) => true,
        Ast::Assertion(a) => a.kind == AssertionKind::StartLine,
        Ast::ClassBracketed(class) => match &class.kind {
            ClassSet::Item(item) => class_item(item),
            _ => false,
        },
        Ast::Group(group) => {
            match &group.kind {
                GroupKind::CaptureIndex(_) => *captures += 1,
                GroupKind::NonCapturing(flags) if flags.items.is_empty() => (),
                _ => return false,
            }
            supported(&group.ast, captures)
        }
        Ast::Repetition(repeat) => {
            repeat.greedy
                && matches!(
                    repeat.op.kind,
                    RepetitionKind::ZeroOrOne
                        | RepetitionKind::ZeroOrMore
                        | RepetitionKind::OneOrMore
                )
                && matches!(
                    repeat.ast.as_ref(),
                    Ast::Literal(_) | Ast::ClassBracketed(_)
                )
                && supported(&repeat.ast, captures)
        }
        Ast::Concat(concat) => concat.asts.iter().all(|a| supported(a, captures)),
        Ast::Alternation(alt) => alt.asts.iter().all(|a| supported(a, captures)),
        _ => false,
    }
}
fn class_item(item: &ClassSetItem) -> bool {
    match item {
        ClassSetItem::Literal(_) | ClassSetItem::Range(_) => true,
        ClassSetItem::Union(union) => union.items.iter().all(class_item),
        _ => false,
    }
}
fn unsupported() -> DataFusionError {
    DataFusionError::Plan("REGEXP_EXTRACT requires one capture in the restricted ASCII literal grammar without flags, escapes, repeated groups or set operations".into())
}
