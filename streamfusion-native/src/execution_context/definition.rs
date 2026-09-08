// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::{
    planner::{persistent, region::RegionPlan},
    proto,
};
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

/// Tree and DAG protocols share lifecycle resources. A DAG never fabricates a nested
/// tree: doing so would duplicate physical identity, mutable state, and metric ownership.
pub(super) enum Definition {
    Tree(proto::NativePlan),
    Region(Arc<RegionPlan>),
}
impl Definition {
    pub(super) fn protocol_version(&self) -> u32 {
        match self {
            Self::Tree(plan) => plan.protocol_version,
            Self::Region(_) => crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION,
        }
    }
    pub(super) fn roots(&self) -> Box<dyn Iterator<Item = &proto::Operator> + '_> {
        match self {
            Self::Tree(plan) => Box::new(plan.root.iter()),
            Self::Region(plan) => Box::new(
                plan.message
                    .stages
                    .iter()
                    .filter_map(|stage| stage.operator.as_ref()),
            ),
        }
    }
    pub(super) fn tree(&self) -> Result<&proto::NativePlan> {
        match self {
            Self::Tree(plan) => Ok(plan),
            Self::Region(_) => Err(DataFusionError::Plan(
                "native region requires the multi-output execution API".into(),
            )),
        }
    }
    pub(super) fn find_unique(
        &self,
        matches: impl Fn(&proto::Operator) -> bool,
    ) -> Result<&proto::Operator> {
        fn visit<'a>(
            node: &'a proto::Operator,
            matches: &impl Fn(&proto::Operator) -> bool,
            found: &mut Option<&'a proto::Operator>,
        ) -> Result<()> {
            if matches(node) && found.replace(node).is_some() {
                return Err(DataFusionError::Plan(
                    "native lifecycle binding matches more than one physical definition".into(),
                ));
            }
            for child in persistent::children(node)? {
                visit(child, matches, found)?;
            }
            Ok(())
        }
        let mut found = None;
        for root in self.roots() {
            visit(root, &matches, &mut found)?;
        }
        found.ok_or_else(|| {
            DataFusionError::Plan(
                "native lifecycle binding has no matching physical definition".into(),
            )
        })
    }
}
