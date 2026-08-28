// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct FlinkInitCapExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkInitCapExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkInitCapExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkInitCapExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "INITCAP({})", self.value)
    }
}

impl PhysicalExpr for FlinkInitCapExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.value.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("INITCAP expected Utf8 input".to_string())
                    })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    strings.iter().map(|value| value.map(flink_init_cap)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.as_deref().map(flink_init_cap)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "INITCAP expected Utf8 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Utf8,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.value]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            value: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "INITCAP(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_init_cap(value: &str) -> String {
    let mut starts_word = true;
    value
        .chars()
        .map(|character| match character {
            '0'..='9' => {
                starts_word = false;
                character
            }
            'A'..='Z' => {
                let output = if starts_word {
                    character
                } else {
                    character.to_ascii_lowercase()
                };
                starts_word = false;
                output
            }
            'a'..='z' => {
                let output = if starts_word {
                    character.to_ascii_uppercase()
                } else {
                    character
                };
                starts_word = false;
                output
            }
            _ => {
                starts_word = true;
                character
            }
        })
        .collect()
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if value.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "INITCAP requires Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkInitCapExpr { value }))
}

#[cfg(test)]
mod tests {
    use super::flink_init_cap;

    #[test]
    fn matches_flink_ascii_word_rules() {
        assert_eq!(flink_init_cap("fLinK 42SQL"), "Flink 42sql");
        assert_eq!(flink_init_cap("éFLINK_rocks"), "éFlink_Rocks");
    }
}
