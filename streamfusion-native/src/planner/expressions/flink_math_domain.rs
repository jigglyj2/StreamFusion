// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! DataFusion computes the function. Flink's Java Math kernels produce negative quiet NaNs for
//! domain errors in ASIN, ACOS and LOG10; the Rust kernels produce positive NaNs on this target.
//! Preserve the observable result bits without evaluating the operand a second time.
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, PrimitiveArray};
use arrow::datatypes::{ArrowPrimitiveType, DataType, FieldRef, Schema};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkDomainMath {
    inner: ScalarUDF,
}
impl ScalarUDFImpl for FlinkDomainMath {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args
            .args
            .first()
            .ok_or_else(|| DataFusionError::Execution("math operand missing".into()))?
            .clone();
        let result = self.inner.invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
                let nan_input = matches!(input, ColumnarValue::Scalar(ScalarValue::Float64(Some(x))) if x.is_nan());
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                    if value.is_nan() && !nan_input {
                        f64::from_bits(0xfff8_0000_0000_0000)
                    } else {
                        value
                    },
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                let nan_input = matches!(input, ColumnarValue::Scalar(ScalarValue::Float32(Some(x))) if x.is_nan());
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(
                    if value.is_nan() && !nan_input {
                        f32::from_bits(0xffc0_0000)
                    } else {
                        value
                    },
                ))))
            }
            ColumnarValue::Array(output) => {
                let input = input.into_array(output.len())?;
                if let (Some(input), Some(output)) = (
                    input.as_any().downcast_ref::<Float64Array>(),
                    output.as_any().downcast_ref::<Float64Array>(),
                ) {
                    return Ok(ColumnarValue::Array(repair(
                        input,
                        output,
                        f64::is_nan,
                        f64::from_bits(0xfff8_0000_0000_0000),
                    )));
                }
                if let (Some(input), Some(output)) = (
                    input.as_any().downcast_ref::<Float32Array>(),
                    output.as_any().downcast_ref::<Float32Array>(),
                ) {
                    return Ok(ColumnarValue::Array(repair(
                        input,
                        output,
                        f32::is_nan,
                        f32::from_bits(0xffc0_0000),
                    )));
                }
                Err(DataFusionError::Execution(
                    "math domain adaptation requires matching floating-point types".into(),
                ))
            }
            value => Ok(value),
        }
    }
}

fn repair<T: ArrowPrimitiveType>(
    input: &PrimitiveArray<T>,
    output: &PrimitiveArray<T>,
    is_nan: fn(T::Native) -> bool,
    negative_nan: T::Native,
) -> ArrayRef {
    let generated = |value: Option<T::Native>, input: Option<T::Native>| {
        value.is_some_and(is_nan) && input.is_some_and(|value| !is_nan(value))
    };
    if !output
        .iter()
        .zip(input.iter())
        .any(|(value, input)| generated(value, input))
    {
        return Arc::new(output.clone());
    }
    Arc::new(PrimitiveArray::<T>::from_iter(
        output.iter().zip(input.iter()).map(|(value, input)| {
            if generated(value, input) {
                Some(negative_nan)
            } else {
                value
            }
        }),
    ))
}

pub(super) fn create(
    function: Arc<ScalarUDF>,
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let function = Arc::new(ScalarUDF::new_from_impl(FlinkDomainMath {
        inner: function.as_ref().clone(),
    }));
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        function,
        vec![operand],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
