// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Keep DataFusion's ProjectionExec and scalar kernels; add Flink admission around
//! supported allocation policies. This is expression ownership, not a fusion driver.

use arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

mod fixed_math;
#[cfg(test)]
mod math_tests;
mod repeat;
#[cfg(test)]
mod tests;

#[derive(Debug)]
struct AdmittedFunction {
    inner: ScalarUDF,
    pool: Arc<dyn MemoryPool>,
    policy: Policy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Policy {
    Repeat,
    FixedMath,
    DateFormat(usize),
    RegexExtract,
    SplitIndex,
}
impl Policy {
    fn label(self) -> &'static str {
        match self {
            Self::Repeat => "native scalar REPEAT workspace and output",
            Self::RegexExtract => "native scalar REGEXP_EXTRACT workspace and output",
            Self::SplitIndex => "native scalar SPLIT_INDEX workspace and output",
            Self::FixedMath => "native scalar fixed math workspace and output",
            Self::DateFormat(_) => "native scalar DATE_FORMAT workspace and output",
        }
    }
    fn workspace(self, args: &ScalarFunctionArgs) -> Result<usize> {
        match self {
            Self::Repeat => repeat::workspace(args),
            Self::RegexExtract => super::regexp_extract::memory::workspace(args),
            Self::SplitIndex => super::string_split_index::memory::workspace(args),
            Self::FixedMath => fixed_math::workspace(args),
            Self::DateFormat(bytes) => args
                .number_rows
                .checked_mul(bytes)
                .and_then(|n| n.checked_add(64 << 10))
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted("DATE_FORMAT workspace overflow".into())
                }),
        }
    }
}
impl PartialEq for AdmittedFunction {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
            && self.policy == other.policy
            && Arc::ptr_eq(&self.pool, &other.pool)
    }
}
impl Eq for AdmittedFunction {}
impl Hash for AdmittedFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        self.policy.hash(state);
        (Arc::as_ptr(&self.pool) as *const () as usize).hash(state);
    }
}
impl ScalarUDFImpl for AdmittedFunction {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<arrow::datatypes::FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn is_strict(&self) -> bool {
        self.inner.inner().is_strict()
    }
    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(inputs)
    }
    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }
    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }
    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }
    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.strictly_order_preserving(inputs)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let memory = MemoryConsumer::new(self.policy.label()).register(&self.pool);
        memory.try_grow(self.policy.workspace(&args)?)?;
        let rows = args.number_rows;
        let result = if rows == 0 {
            arrow::array::new_empty_array(args.return_field.data_type())
        } else {
            self.inner.invoke_with_args(args)?.into_array(rows)?
        };
        let retained = result.get_array_memory_size();
        if retained > memory.size() {
            return Err(DataFusionError::ResourcesExhausted(
                "native scalar result exceeded its admitted workspace".into(),
            ));
        }
        memory.try_resize(retained)?;
        Ok(ColumnarValue::Array(
            crate::memory_pool::arrow_lease::datafusion_array_registered(
                result,
                memory,
                crate::memory_pool::buffer_registry(&self.pool),
            )?,
        ))
    }
}

pub(crate) fn install(
    expression: Arc<dyn PhysicalExpr>,
    pool: Option<&Arc<dyn MemoryPool>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let Some(pool) = pool else {
        return Ok(expression);
    };
    let expression = expression
        .transform_up(|expression| {
            let Some(function) = expression.downcast_ref::<ScalarFunctionExpr>() else {
                return super::managed_expression::install(expression, pool, schema);
            };
            let policy = if let Some(format) = function
                .fun()
                .inner()
                .downcast_ref::<super::date_format::NumericDateFormat>(
            ) {
                Policy::DateFormat(format.bytes_per_row()?)
            } else if function
                .fun()
                .inner()
                .downcast_ref::<super::regexp_extract::RegexExtract>()
                .is_some()
            {
                Policy::RegexExtract
            } else if function
                .fun()
                .inner()
                .downcast_ref::<super::string_split_index::SplitIndex>()
                .is_some()
            {
                Policy::SplitIndex
            } else if function.name() == "repeat" {
                Policy::Repeat
            } else if fixed_math::supports(function, schema)? {
                Policy::FixedMath
            } else {
                return Ok(Transformed::no(expression));
            };
            let admitted = ScalarUDF::new_from_impl(AdmittedFunction {
                inner: function.fun().clone(),
                pool: pool.clone(),
                policy,
            });
            let result = ScalarFunctionExpr::new(
                function.name(),
                Arc::new(admitted),
                function.args().to_vec(),
                function.return_field(schema)?,
                Arc::new(function.config_options().clone()),
            );
            Ok(Transformed::yes(Arc::new(result) as Arc<dyn PhysicalExpr>))
        })
        .data()?;
    super::managed_expression::scope_conditionals(expression)
}
