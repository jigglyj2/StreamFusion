// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::{NativeExecutionContext, Ordering};
use datafusion::error::{DataFusionError, Result};

/// Claim the whole invocation, including lowering and input replacement, before executing any
/// stage. Setup failures may retry; failure after execution begins poisons persistent regions.
pub(super) struct InvocationGuard<'a> {
    context: &'a NativeExecutionContext,
    executing: bool,
    armed: bool,
    pub(super) successful: bool,
}

impl<'a> InvocationGuard<'a> {
    pub(super) fn begin(context: &'a NativeExecutionContext) -> Result<Self> {
        context
            .invocation
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                DataFusionError::Execution(
                    "native plan already has an active or failed invocation".into(),
                )
            })?;
        Ok(Self {
            context,
            executing: false,
            armed: true,
            successful: false,
        })
    }

    pub(super) fn executing(&mut self) {
        self.executing = true;
    }

    pub(super) fn transfer_to_stream(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvocationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.context
                .finish_invocation(!self.executing || self.successful);
        }
    }
}

impl NativeExecutionContext {
    pub(super) fn finish_invocation(&self, successful: bool) {
        // Drop streams before calling this method. Their destructors may release state work;
        // a new invocation must not be admitted until both that work and all inputs are gone.
        let mut poisoned = self.controls.clear().is_err();
        match self.physical_plan.lock() {
            Ok(cached) => {
                if let Some(cached) = cached.as_ref() {
                    for input in &cached.inputs {
                        input.clear();
                    }
                }
            }
            Err(_) => poisoned = true,
        }
        self.invocation.store(
            if poisoned
                || (!successful
                    && (!self.persistent.is_empty()
                        || matches!(self.plan, super::Definition::Region(_))))
            {
                2
            } else {
                0
            },
            Ordering::Release,
        );
    }
}
