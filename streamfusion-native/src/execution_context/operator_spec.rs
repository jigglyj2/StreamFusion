// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Configuration admission/copy from borrowed, already decoded protobufs. Physical children
//! stay exclusively in the shared tree, not in every persistent state's retained descriptor.

use datafusion::error::{DataFusionError, Result};

trait ConfigurationMemory {
    fn configuration_memory(&self, shallow: bool) -> Result<usize>;
}

#[allow(unused_variables, unused_mut)]
mod generated {
    use super::*;
    include!(concat!(env!("OUT_DIR"), "/operator_specs.rs"));
}

pub(crate) fn admission(node: &crate::proto::Operator) -> Result<usize> {
    // Own configuration clone and serialized constructor bytes can coexist. The constructor
    // separately reserves its decoded/retained representation before the temporary lease ends.
    node.configuration_memory(true)?
        .checked_mul(2)
        .ok_or_else(overflow)
}

pub(crate) fn without_children(node: &crate::proto::Operator) -> crate::proto::Operator {
    generated::without_children(node)
}

fn add(bytes: &mut usize, count: usize) -> Result<()> {
    *bytes = bytes.checked_add(count).ok_or_else(overflow)?;
    Ok(())
}
fn add_payload(bytes: &mut usize, length: usize) -> Result<()> {
    add(
        bytes,
        length
            .checked_mul(2)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(overflow)?,
    )
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("native operator configuration admission overflow".into())
}

#[cfg(test)]
mod tests;
