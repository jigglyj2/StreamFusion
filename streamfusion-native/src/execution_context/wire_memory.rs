// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Allocation-free admission scan of the versioned plan wire format. The field/message
//! table comes from protoc descriptors at build time. Byte payloads are not interpreted
//! recursively and unknown fields are skipped, just as in protobuf decoding.

use datafusion::error::{DataFusionError, Result};

#[derive(Clone, Copy)]
enum Kind {
    Message(usize),
    Bytes,
    Scalar,
}
struct Layout {
    inline: usize,
    fields: &'static [(u32, Kind)],
}
include!(concat!(env!("OUT_DIR"), "/plan_memory_layout.rs"));

#[cfg(test)]
mod tests;

#[derive(Default, Debug)]
pub(crate) struct PlanMemory {
    structural: usize,
    payload: usize,
}
impl PlanMemory {
    pub(crate) fn scan(bytes: &[u8]) -> Result<Self> {
        let mut estimate = Self::default();
        estimate.message(bytes, ROOT, 0)?;
        Ok(estimate)
    }
    pub(crate) fn scan_region(bytes: &[u8]) -> Result<Self> {
        let mut estimate = Self::default();
        estimate.message(bytes, REGION_ROOT, 0)?;
        Ok(estimate)
    }
    pub(crate) fn decoded(&self) -> Result<usize> {
        self.total(1)
    }
    pub(crate) fn physical(&self) -> Result<usize> {
        self.total(2)
    }
    fn total(&self, copies: usize) -> Result<usize> {
        self.structural
            .checked_mul(copies)
            .and_then(|n| self.payload.checked_mul(2)?.checked_add(n))
            .ok_or_else(overflow)
    }
    fn message(&mut self, mut bytes: &[u8], layout: usize, depth: usize) -> Result<()> {
        if depth >= 100 {
            return Err(invalid("plan protobuf recursion limit exceeded"));
        }
        let layout = &LAYOUTS[layout];
        // A singleton repeated message can allocate four inline elements; Box/Arc headers
        // and vector control/storage have slack too. This counts all messages, not just
        // physical operators, so compact repeated expressions cannot evade admission.
        add(
            &mut self.structural,
            layout
                .inline
                .checked_mul(4)
                .and_then(|n| n.checked_add(64))
                .ok_or_else(overflow)?,
        )?;
        while !bytes.is_empty() {
            let tag = varint(&mut bytes)?;
            let number = u32::try_from(tag >> 3).map_err(|_| invalid("plan field tag overflow"))?;
            if number == 0 {
                return Err(invalid("plan field number is zero"));
            }
            let kind = layout
                .fields
                .iter()
                .find(|(field, _)| *field == number)
                .map(|(_, kind)| *kind);
            match tag & 7 {
                0 => {
                    varint(&mut bytes)?;
                    if kind.is_some() {
                        add(&mut self.structural, 32)?;
                    }
                }
                1 => {
                    take(&mut bytes, 8)?;
                    if kind.is_some() {
                        add(&mut self.structural, 32)?;
                    }
                }
                5 => {
                    take(&mut bytes, 4)?;
                    if kind.is_some() {
                        add(&mut self.structural, 32)?;
                    }
                }
                2 => {
                    let len = usize::try_from(varint(&mut bytes)?)
                        .map_err(|_| invalid("plan field length overflow"))?;
                    let value = take(&mut bytes, len)?;
                    match kind {
                        Some(Kind::Message(child)) => self.message(value, child, depth + 1)?,
                        Some(Kind::Bytes) => {
                            add(&mut self.payload, len)?;
                            add(&mut self.structural, 128)?;
                        }
                        Some(Kind::Scalar) => add(
                            &mut self.structural,
                            len.checked_mul(32).ok_or_else(overflow)?,
                        )?,
                        None => {}
                    }
                }
                _ => return Err(invalid("unsupported plan protobuf wire type")),
            }
        }
        Ok(())
    }
}

fn add(total: &mut usize, value: usize) -> Result<()> {
    *total = total.checked_add(value).ok_or_else(overflow)?;
    Ok(())
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("native plan admission size overflow".into())
}
fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
fn take<'a>(bytes: &mut &'a [u8], size: usize) -> Result<&'a [u8]> {
    if size > bytes.len() {
        return Err(invalid("truncated plan protobuf field"));
    }
    let (head, tail) = bytes.split_at(size);
    *bytes = tail;
    Ok(head)
}
fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(invalid("plan protobuf varint overflow"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(value);
        }
    }
    Err(invalid("invalid plan protobuf varint"))
}
