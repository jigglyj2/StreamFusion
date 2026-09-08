// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl NativeExecutionContext {
    pub(crate) fn metric_value(&self, plan_node_id: u64, name: &str) -> Result<usize> {
        fn sum_all(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
            let current = plan
                .metrics()
                .and_then(|metrics| metrics.sum_by_name(name))
                .map(|value| value.as_usize())
                .unwrap_or(0);
            current.saturating_add(
                plan.children()
                    .into_iter()
                    .map(|child| sum_all(child, name))
                    .sum(),
            )
        }

        fn sum_stage(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
            if plan
                .downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                .is_some()
            {
                return 0;
            }
            let current = plan
                .metrics()
                .and_then(|metrics| metrics.sum_by_name(name))
                .map(|value| value.as_usize())
                .unwrap_or(0);
            current.saturating_add(
                plan.children()
                    .into_iter()
                    .map(|child| sum_stage(child, name))
                    .sum(),
            )
        }

        fn identified_metric(
            plan: &Arc<dyn ExecutionPlan>,
            plan_node_id: u64,
            name: &str,
        ) -> Option<usize> {
            if let Some(identified) =
                plan.downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
            {
                if identified.plan_node_id() == plan_node_id {
                    return Some(sum_stage(identified.input(), name));
                }
            }
            plan.children()
                .into_iter()
                .find_map(|child| identified_metric(child, plan_node_id, name))
        }

        let cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        let Some(cached) = cached.as_ref() else {
            return Ok(0);
        };
        match &cached.plan {
            PreparedPlan::Tree(plan) => {
                if plan_node_id == 0 {
                    return Ok(sum_all(plan, name));
                }
                identified_metric(plan, plan_node_id, name)
            }
            PreparedPlan::Region(region) => {
                let metric = |plan: &Arc<dyn ExecutionPlan>| {
                    let stage = plan
                        .downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                        .unwrap();
                    sum_stage(stage.input(), name)
                };
                if plan_node_id == 0 {
                    return Ok(region.stages().iter().map(metric).sum());
                }
                region
                    .stages()
                    .iter()
                    .find(|plan| {
                        plan.downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                            .unwrap()
                            .plan_node_id()
                            == plan_node_id
                    })
                    .map(metric)
            }
        }
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "native metric requested unknown plan node {plan_node_id}"
            ))
        })
    }

    pub(crate) fn metric_snapshot(&self) -> Result<Vec<i64>> {
        let cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        Ok(cached
            .as_ref()
            .map_or_else(Vec::new, |cached| match &cached.plan {
                PreparedPlan::Tree(plan) => crate::plan_metrics::snapshot(plan),
                PreparedPlan::Region(region) => region.metrics(),
            }))
    }

    pub(crate) fn input_batch_count(&self, ids: &[u64]) -> Result<u64> {
        fn count(plan: &Arc<dyn ExecutionPlan>, ids: &[u64]) -> u64 {
            let own = plan
                .downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                .filter(|stage| ids.contains(&stage.plan_node_id()))
                .map_or(0, |stage| stage.input_batches());
            plan.children()
                .into_iter()
                .fold(own, |total, child| total.saturating_add(count(child, ids)))
        }
        let cached = self
            .physical_plan
            .lock()
            .map_err(|_| DataFusionError::Internal("native metric-tree lock poisoned".into()))?;
        Ok(cached.as_ref().map_or(0, |cached| match &cached.plan {
            PreparedPlan::Tree(plan) => count(plan, ids),
            PreparedPlan::Region(region) => region
                .stages()
                .iter()
                .filter_map(|plan| {
                    plan.downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                })
                .filter(|stage| ids.contains(&stage.plan_node_id()))
                .fold(0u64, |total, stage| {
                    total.saturating_add(stage.input_batches())
                }),
        }))
    }
}
