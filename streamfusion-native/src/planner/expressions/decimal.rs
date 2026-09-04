// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, Decimal128Array, Decimal128Builder, Int16Array, Int32Array, Int64Array, Int8Array,
};
use arrow::datatypes::{i256, DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

const FLINK_DIVIDE_PRECISION: u32 = 38;
const I256_POWERS_OF_TEN: [i256; 77] = i256_powers_of_ten();

const fn i256_powers_of_ten() -> [i256; 77] {
    let mut powers = [i256::ZERO; 77];
    powers[0] = i256::from_i128(1);
    let mut index = 1;
    while index < powers.len() {
        powers[index] = powers[index - 1].wrapping_mul(i256::from_i128(10));
        index += 1;
    }
    powers
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum DecimalOperation {
    Cast,
    Divide,
}

#[derive(Debug, Eq)]
struct FlinkDecimalExpr {
    operation: DecimalOperation,
    left: Arc<dyn PhysicalExpr>,
    right: Option<Arc<dyn PhysicalExpr>>,
    left_type: DataType,
    left_scale: i8,
    right_scale: i8,
    target_precision: u8,
    target_scale: i8,
}

impl PartialEq for FlinkDecimalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.left.eq(&other.left)
            && self.right == other.right
            && self.left_type == other.left_type
            && self.left_scale == other.left_scale
            && self.right_scale == other.right_scale
            && self.target_precision == other.target_precision
            && self.target_scale == other.target_scale
    }
}

impl Hash for FlinkDecimalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operation.hash(state);
        self.left.hash(state);
        self.right.hash(state);
        self.left_type.hash(state);
        self.left_scale.hash(state);
        self.right_scale.hash(state);
        self.target_precision.hash(state);
        self.target_scale.hash(state);
    }
}

impl std::fmt::Display for FlinkDecimalExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.right.as_ref() {
            Some(right) => write!(formatter, "FLINK_DECIMAL_DIVIDE({}, {})", self.left, right),
            None => write!(formatter, "FLINK_DECIMAL_CAST({})", self.left),
        }
    }
}

impl PhysicalExpr for FlinkDecimalExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Decimal128(
            self.target_precision,
            self.target_scale,
        ))
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let right_nullable = match self.right.as_ref() {
            Some(right) => right.nullable(input_schema)?,
            None => false,
        };
        Ok(self.left.nullable(input_schema)? || right_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left = self.left.evaluate(batch)?.into_array(batch.num_rows())?;
        let right_value = self
            .right
            .as_ref()
            .map(|right| right.evaluate(batch))
            .transpose()?
            .map(|value| value.into_array(batch.num_rows()))
            .transpose()?;
        let right = right_value
            .as_ref()
            .map(|array| decimal_array(array.as_ref(), "right"))
            .transpose()?;

        let mut output = Decimal128Builder::with_capacity(batch.num_rows());
        for index in 0..batch.num_rows() {
            if left.is_null(index) || right.is_some_and(|array| array.is_null(index)) {
                output.append_null();
                continue;
            }
            let value = match right {
                Some(right) => flink_divide(
                    decimal_array(left.as_ref(), "left")?.value(index),
                    self.left_scale,
                    right.value(index),
                    self.right_scale,
                    self.target_precision,
                    self.target_scale,
                )?,
                None => flink_rescale(
                    numeric_unscaled(left.as_ref(), index, &self.left_type)?,
                    self.left_scale,
                    self.target_precision,
                    self.target_scale,
                ),
            };
            match value {
                Some(value) => output.append_value(value),
                None => output.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            output
                .finish()
                .with_precision_and_scale(self.target_precision, self.target_scale)?,
        )))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.left.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![&self.left];
        if let Some(right) = self.right.as_ref() {
            children.push(right);
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let expected = if self.right.is_some() { 2 } else { 1 };
        if children.len() != expected {
            return Err(DataFusionError::Internal(format!(
                "Flink decimal expression expected {expected} children, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            operation: self.operation,
            left: Arc::clone(&children[0]),
            right: (expected == 2).then(|| Arc::clone(&children[1])),
            left_type: self.left_type.clone(),
            left_scale: self.left_scale,
            right_scale: self.right_scale,
            target_precision: self.target_precision,
            target_scale: self.target_scale,
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

pub(crate) fn cast(
    operand: Arc<dyn PhysicalExpr>,
    target: DataType,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let left_type = operand.data_type(schema)?;
    let left_scale = match left_type {
        DataType::Decimal128(_, scale) => scale,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => 0,
        ref other => {
            return Err(DataFusionError::Plan(format!(
                "Flink decimal cast source must be a signed integer or DECIMAL, got {other}"
            )))
        }
    };
    let (target_precision, target_scale) = decimal_type(&target, "cast target")?;
    Ok(Arc::new(FlinkDecimalExpr {
        operation: DecimalOperation::Cast,
        left: operand,
        right: None,
        left_type,
        left_scale,
        right_scale: 0,
        target_precision,
        target_scale,
    }))
}

pub(crate) fn divide(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    target: DataType,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (_, left_scale) = decimal_type(&left.data_type(schema)?, "division left operand")?;
    let left_type = left.data_type(schema)?;
    let (_, right_scale) = decimal_type(&right.data_type(schema)?, "division right operand")?;
    let (target_precision, target_scale) = decimal_type(&target, "division result")?;
    Ok(Arc::new(FlinkDecimalExpr {
        operation: DecimalOperation::Divide,
        left,
        right: Some(right),
        left_type,
        left_scale,
        right_scale,
        target_precision,
        target_scale,
    }))
}

fn numeric_unscaled(array: &dyn Array, index: usize, data_type: &DataType) -> Result<i128> {
    macro_rules! signed_value {
        ($array_type:ty) => {
            array
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Flink decimal cast evaluated as {}, expected {data_type}",
                        array.data_type()
                    ))
                })?
                .value(index) as i128
        };
    }
    Ok(match data_type {
        DataType::Int8 => signed_value!(Int8Array),
        DataType::Int16 => signed_value!(Int16Array),
        DataType::Int32 => signed_value!(Int32Array),
        DataType::Int64 => signed_value!(Int64Array),
        DataType::Decimal128(_, _) => decimal_array(array, "cast source")?.value(index),
        other => {
            return Err(DataFusionError::Execution(format!(
                "unsupported Flink decimal cast source {other}"
            )))
        }
    })
}

fn decimal_type(data_type: &DataType, description: &str) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) if *precision <= 38 && *scale >= 0 => {
            Ok((*precision, *scale))
        }
        other => Err(DataFusionError::Plan(format!(
            "Flink decimal {description} must be DECIMAL(1..38, 0..precision), got {other}"
        ))),
    }
}

fn decimal_array<'a>(array: &'a dyn Array, description: &str) -> Result<&'a Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Flink decimal {description} evaluated as {}, expected Decimal128",
                array.data_type()
            ))
        })
}

fn flink_rescale(value: i128, source_scale: i8, precision: u8, target_scale: i8) -> Option<i128> {
    let value = i256::from_i128(value);
    let rescaled = if target_scale >= source_scale {
        value.checked_mul(pow10_i256((target_scale - source_scale) as u32)?)?
    } else {
        round_half_up_i256(value, pow10_i256((source_scale - target_scale) as u32)?)?
    };
    checked_decimal_i256(rescaled, precision)
}

fn flink_divide(
    left: i128,
    left_scale: i8,
    right: i128,
    right_scale: i8,
    precision: u8,
    target_scale: i8,
) -> Result<Option<i128>> {
    if right == 0 {
        return Err(DataFusionError::Execution("Division by zero".to_string()));
    }
    Ok(flink_divide_nonzero(
        left,
        left_scale,
        right,
        right_scale,
        precision,
        target_scale,
    ))
}

pub(crate) fn flink_divide_nonzero(
    left: i128,
    left_scale: i8,
    right: i128,
    right_scale: i8,
    precision: u8,
    target_scale: i8,
) -> Option<i128> {
    debug_assert_ne!(right, 0);
    if left == 0 {
        return Some(0);
    }

    if let Some(result) = flink_divide_nonzero_i256(
        left,
        left_scale,
        right,
        right_scale,
        precision,
        target_scale,
    ) {
        return result;
    }

    flink_divide_nonzero_bigint(
        left,
        left_scale,
        right,
        right_scale,
        precision,
        target_scale,
    )
}

fn flink_divide_nonzero_bigint(
    left: i128,
    left_scale: i8,
    right: i128,
    right_scale: i8,
    precision: u8,
    target_scale: i8,
) -> Option<i128> {
    let negative = (left < 0) != (right < 0);
    let numerator = BigInt::from(left).abs() * pow10(right_scale as u32);
    let denominator = BigInt::from(right).abs() * pow10(left_scale as u32);
    let exponent = decimal_exponent(&numerator, &denominator);
    let significant_shift = i64::from(FLINK_DIVIDE_PRECISION - 1) - exponent;
    let significant = if significant_shift >= 0 {
        round_half_up(numerator * pow10(significant_shift as u32), denominator)
    } else {
        round_half_up(numerator, denominator * pow10((-significant_shift) as u32))
    };

    let output_shift = exponent - i64::from(FLINK_DIVIDE_PRECISION - 1) + i64::from(target_scale);
    let mut unscaled = if output_shift >= 0 {
        significant * pow10(output_shift as u32)
    } else {
        round_half_up(significant, pow10((-output_shift) as u32))
    };
    if negative {
        unscaled = -unscaled;
    }
    checked_decimal(unscaled, precision)
}

fn flink_divide_nonzero_i256(
    left: i128,
    left_scale: i8,
    right: i128,
    right_scale: i8,
    precision: u8,
    target_scale: i8,
) -> Option<Option<i128>> {
    let negative = (left < 0) != (right < 0);
    let numerator = i256::from_i128(left)
        .checked_abs()?
        .checked_mul(pow10_i256(right_scale as u32)?)?;
    let denominator = i256::from_i128(right)
        .checked_abs()?
        .checked_mul(pow10_i256(left_scale as u32)?)?;
    let exponent = decimal_exponent_i256(numerator, denominator)?;
    let significant_shift = i64::from(FLINK_DIVIDE_PRECISION - 1) - exponent;
    let significant = if significant_shift >= 0 {
        round_half_up_i256(
            numerator.checked_mul(pow10_i256(significant_shift as u32)?)?,
            denominator,
        )?
    } else {
        let scaled_denominator = denominator.checked_mul(pow10_i256((-significant_shift) as u32)?);
        match scaled_denominator {
            Some(denominator) => round_half_up_i256(numerator, denominator)?,
            None => return None,
        }
    };

    let output_shift = exponent - i64::from(FLINK_DIVIDE_PRECISION - 1) + i64::from(target_scale);
    let mut unscaled = if output_shift >= 0 {
        match significant.checked_mul(pow10_i256(output_shift as u32)?) {
            Some(value) => value,
            None => return None,
        }
    } else {
        round_half_up_i256(significant, pow10_i256((-output_shift) as u32)?)?
    };
    if negative {
        unscaled = unscaled.checked_neg()?;
    }
    Some(checked_decimal_i256(unscaled, precision))
}

fn decimal_exponent_i256(numerator: i256, denominator: i256) -> Option<i64> {
    let numerator_digits = i64::from(numerator.checked_ilog10()?) + 1;
    let denominator_digits = i64::from(denominator.checked_ilog10()?) + 1;
    let candidate = numerator_digits - denominator_digits;
    if candidate >= 0 {
        let scaled_denominator = denominator.checked_mul(pow10_i256(candidate as u32)?)?;
        Some(if numerator < scaled_denominator {
            candidate - 1
        } else {
            candidate
        })
    } else {
        let scaled_numerator = numerator.checked_mul(pow10_i256((-candidate) as u32)?)?;
        Some(if scaled_numerator < denominator {
            candidate - 1
        } else {
            candidate
        })
    }
}

fn round_half_up_i256(numerator: i256, denominator: i256) -> Option<i256> {
    debug_assert!(denominator > i256::ZERO);
    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?;
    let round = remainder.checked_abs()?.checked_mul(i256::from_i128(2))? >= denominator;
    if !round {
        return Some(quotient);
    }
    if numerator < i256::ZERO {
        quotient.checked_sub(i256::from_i128(1))
    } else {
        quotient.checked_add(i256::from_i128(1))
    }
}

fn checked_decimal_i256(value: i256, precision: u8) -> Option<i128> {
    if value.checked_abs()? >= pow10_i256(precision.into())? {
        return None;
    }
    value.to_i128()
}

fn pow10_i256(power: u32) -> Option<i256> {
    I256_POWERS_OF_TEN.get(power as usize).copied()
}

fn decimal_exponent(numerator: &BigInt, denominator: &BigInt) -> i64 {
    let numerator_digits = numerator.to_str_radix(10).len() as i64;
    let denominator_digits = denominator.to_str_radix(10).len() as i64;
    let candidate = numerator_digits - denominator_digits;
    if candidate >= 0 {
        if numerator < &(denominator * pow10(candidate as u32)) {
            candidate - 1
        } else {
            candidate
        }
    } else if &(numerator * pow10((-candidate) as u32)) < denominator {
        candidate - 1
    } else {
        candidate
    }
}

fn round_half_up(numerator: BigInt, denominator: BigInt) -> BigInt {
    debug_assert!(denominator > BigInt::zero());
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    if remainder.abs() * 2 >= denominator {
        if numerator.is_negative() {
            quotient - 1
        } else {
            quotient + 1
        }
    } else {
        quotient
    }
}

fn checked_decimal(value: BigInt, precision: u8) -> Option<i128> {
    if value.abs() >= pow10(precision.into()) {
        return None;
    }
    value.to_i128()
}

fn pow10(power: u32) -> BigInt {
    BigInt::from(10_u8).pow(power)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rescale_rounds_half_away_from_zero_and_returns_null_on_overflow() {
        assert_eq!(flink_rescale(1245, 3, 3, 2), Some(125));
        assert_eq!(flink_rescale(-1245, 3, 3, 2), Some(-125));
        assert_eq!(flink_rescale(999, 0, 3, 1), None);
    }

    #[test]
    fn divide_applies_math_context_before_result_scale() {
        assert_eq!(flink_divide(1, 0, 3, 0, 10, 6).unwrap(), Some(333_333));
        assert_eq!(flink_divide(-2, 0, 3, 0, 10, 6).unwrap(), Some(-666_667));
        assert_eq!(flink_divide(1, 2, 8, 1, 10, 6).unwrap(), Some(12_500));
        assert_eq!(flink_divide(999, 0, 1, 0, 2, 0).unwrap(), None);
    }

    #[test]
    fn divide_reports_zero_only_for_nonnull_operands() {
        assert!(flink_divide(1, 0, 0, 0, 10, 2).is_err());
    }

    #[test]
    fn fixed_width_divide_matches_bigint_reference() {
        let values = [
            -((10_i128.pow(38)) - 1),
            -123_456_789_012_345_678,
            -3,
            1,
            7,
            123_456_789_012_345_678,
            (10_i128.pow(38)) - 1,
        ];
        let scales = [0_i8, 2, 18, 38];
        let target_scales = [0_i8, 6, 18, 38];
        let mut fixed_width_cases = 0;
        for left in values {
            for right in values.into_iter().filter(|right| *right != 0) {
                for left_scale in scales {
                    for right_scale in scales {
                        for target_scale in target_scales {
                            if let Some(actual) = flink_divide_nonzero_i256(
                                left,
                                left_scale,
                                right,
                                right_scale,
                                38,
                                target_scale,
                            ) {
                                assert_eq!(
                                    actual,
                                    flink_divide_nonzero_bigint(
                                        left,
                                        left_scale,
                                        right,
                                        right_scale,
                                        38,
                                        target_scale,
                                    )
                                );
                                fixed_width_cases += 1;
                            }
                        }
                    }
                }
            }
        }
        assert!(fixed_width_cases > 1_000);
        assert!(flink_divide_nonzero_i256(123_456_789_012, 2, 17, 0, 38, 6).is_some());
    }
}
