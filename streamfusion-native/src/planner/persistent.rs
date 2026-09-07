// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::proto;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub(crate) mod control;
pub(crate) mod gauges;
pub(crate) mod unary;

/// Task-lifetime execution resources are bound once to a Java-planned node. These may own
/// keyed state or a replayable local buffer; checkpoint methods apply only to keyed owners.
/// Builders implement one operator, not combinations of adjacent operators. Recursive lowering
/// supplies native children and the common stream/control/metric driver owns execution.
pub(crate) trait PersistentOperatorFactory: Send + Sync {
    /// Stable names/types/scopes for the binding lifetime. Values are sampled together at a
    /// plan edge; metric reporters never cross JNI individually or enter an operator row loop.
    fn gauge_definitions(&self) -> Result<&'static [gauges::GaugeDefinition]> {
        Ok(&[])
    }
    fn write_gauge_values(&self, values: &mut [i64]) -> Result<()> {
        if values.is_empty() {
            Ok(())
        } else {
            Err(DataFusionError::Execution(
                "native gauge binding has no value writer".into(),
            ))
        }
    }

    /// An owned timestamp envelope cannot silently be treated as a SQL payload or arrival ordinal.
    fn supports_owned_envelope(&self) -> bool {
        false
    }

    /// Reject unmigrated control semantics before opening an invocation or mutating state.
    /// This capability is stable for the binding lifetime and depends on event kind, not value.
    fn supports_control(&self, _event: control::ControlEvent) -> bool {
        false
    }

    fn build(
        &self,
        node: &proto::Operator,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn snapshot(&self, _key_group: u32) -> Result<crate::state::SnapshotBytes> {
        Err(DataFusionError::Plan(
            "persistent node has no shared snapshot binding".into(),
        ))
    }

    fn restore(&self, _key_group: u32, _bytes: &[u8]) -> Result<()> {
        Err(DataFusionError::Plan(
            "persistent node has no shared restore binding".into(),
        ))
    }

    fn checkpoint(&self, _directory: &std::path::Path) -> Result<()> {
        Err(DataFusionError::Plan(
            "persistent node has no shared checkpoint binding".into(),
        ))
    }
}

pub(crate) type PersistentBinding = (u64, Arc<dyn PersistentOperatorFactory>);

/// The current versioned protobuf nests children in operator messages. Keep this schema adapter
/// centralized; region execution must never enumerate supported operator combinations.
pub(crate) fn children(node: &proto::Operator) -> Result<Vec<&proto::Operator>> {
    use proto::operator::Operator::*;
    let child = match node.operator.as_ref() {
        Some(Input(_) | Values(_)) => return Ok(Vec::new()),
        Some(Union(node)) => return Ok(node.inputs.iter().collect()),
        Some(RegularJoin(node)) => {
            return [node.left_input.as_deref(), node.right_input.as_deref()]
                .into_iter()
                .map(required)
                .collect()
        }
        Some(Calc(node)) => node.input.as_deref(),
        Some(Expand(node)) => node.input.as_deref(),
        Some(ArrayUnnest(node)) => node.input.as_deref(),
        Some(ReplicateRows(node)) => node.input.as_deref(),
        Some(Deduplicate(node)) => node.input.as_deref(),
        Some(GroupAggregate(node)) => node.input.as_deref(),
        Some(LocalGroupAggregate(node)) => node.input.as_deref(),
        Some(GlobalGroupAggregate(node)) => node.input.as_deref(),
        Some(IncrementalGroupAggregate(node)) => node.input.as_deref(),
        Some(WindowAggregate(node)) => node.input.as_deref(),
        Some(LocalWindowAggregate(node)) => node.input.as_deref(),
        Some(WindowDeduplicate(node)) => node.input.as_deref(),
        Some(WindowRank(node)) => node.input.as_deref(),
        Some(WindowTableFunction(node)) => node.input.as_deref(),
        Some(TopN(node)) => node.input.as_deref(),
        Some(OverAggregate(node)) => node.input.as_deref(),
        Some(ChangelogNormalize(node)) => node.input.as_deref(),
        Some(TemporalSort(node)) => node.input.as_deref(),
        Some(BoundedSort(node)) => node.input.as_deref(),
        Some(BoundedRank(node)) => node.input.as_deref(),
        // These legacy handle contracts still need explicit child edges before generic lowering.
        Some(
            WindowJoin(_) | IntervalJoin(_) | TemporalJoin(_) | MultiJoin(_) | MatchRecognize(_),
        ) => {
            return Err(DataFusionError::Plan(
                "native operator contract does not yet specify physical child edges".into(),
            ))
        }
        None => return Err(DataFusionError::Plan("native operator is empty".into())),
    };
    Ok(vec![required(child)?])
}
fn required(child: Option<&proto::Operator>) -> Result<&proto::Operator> {
    child.ok_or_else(|| {
        DataFusionError::Plan("native operator requires an explicit physical child".into())
    })
}

pub(crate) fn find_unique<'a>(
    plan: &'a proto::NativePlan,
    matches: impl Fn(&proto::Operator) -> bool,
) -> Result<&'a proto::Operator> {
    fn visit<'a>(
        node: &'a proto::Operator,
        matches: &impl Fn(&proto::Operator) -> bool,
        found: &mut Option<&'a proto::Operator>,
    ) -> Result<()> {
        if matches(node) && found.replace(node).is_some() {
            return Err(DataFusionError::Plan("legacy state handle requires exactly one matching persistent node; use multiple bindings in the shared execution context".into()));
        }
        for child in children(node)? {
            visit(child, matches, found)?;
        }
        Ok(())
    }
    let mut found = None;
    visit(
        plan.root
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("native plan has no root".into()))?,
        &matches,
        &mut found,
    )?;
    found.ok_or_else(|| DataFusionError::Plan("native plan has no matching persistent node".into()))
}
