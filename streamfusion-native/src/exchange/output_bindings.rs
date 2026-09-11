// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::prepared_router::PreparedRouter;
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

pub(crate) struct PortOutputs {
    pub(crate) arrow: bool,
    pub(crate) frames: Vec<(usize, Arc<PreparedRouter>)>,
}
pub(crate) struct OutputBindings {
    pub(crate) ports: Vec<PortOutputs>,
}
impl OutputBindings {
    pub(crate) fn new(arrow: &[bool], routers: Vec<(usize, Arc<PreparedRouter>)>) -> Result<Self> {
        let mut ports = arrow
            .iter()
            .map(|&arrow| PortOutputs {
                arrow,
                frames: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (id, (port, router)) in routers.into_iter().enumerate() {
            router.validate_native_output()?;
            ports
                .get_mut(port)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "exchange output binding addresses an unknown port".into(),
                    )
                })?
                .frames
                .push((id, router));
        }
        Ok(Self { ports })
    }
}
