// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct FlinkStringExtremumExpr {
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    greatest: bool,
}

impl PartialEq for FlinkStringExtremumExpr {
    fn eq(&self, other: &Self) -> bool {
        self.greatest == other.greatest && self.arguments == other.arguments
    }
}

impl Hash for FlinkStringExtremumExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.greatest.hash(state);
        self.arguments.hash(state);
    }
}

impl std::fmt::Display for FlinkStringExtremumExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = if self.greatest { "GREATEST" } else { "LEAST" };
        write!(formatter, "{name}({} arguments)", self.arguments.len())
    }
}

impl PhysicalExpr for FlinkStringExtremumExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arguments.iter().try_fold(false, |nullable, argument| {
            Ok(nullable || argument.nullable(input_schema)?)
        })
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arrays = self
            .arguments
            .iter()
            .map(|argument| argument.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        let strings = arrays
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("GREATEST/LEAST expected Utf8 arguments".to_string())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            (0..batch.num_rows()).map(|row| {
                strings
                    .iter()
                    .map(|array| (!array.is_null(row)).then(|| array.value(row)))
                    .try_fold(None, |selected, value| {
                        let value = value?;
                        Some(Some(match selected {
                            None => value,
                            Some(selected) if choose(value, selected, self.greatest) => value,
                            Some(selected) => selected,
                        }))
                    })
                    .flatten()
            }),
        ))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "string_extremum",
            DataType::Utf8,
            self.nullable(input_schema)?,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.arguments.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            arguments: children,
            greatest: self.greatest,
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

fn choose(candidate: &str, selected: &str, greatest: bool) -> bool {
    let ordering = candidate.encode_utf16().cmp(selected.encode_utf16());
    if greatest {
        ordering == Ordering::Greater
    } else {
        ordering == Ordering::Less
    }
}

pub(crate) fn create(
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    greatest: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if arguments.is_empty()
        || arguments
            .iter()
            .any(|argument| argument.data_type(schema).ok() != Some(DataType::Utf8))
    {
        return Err(DataFusionError::Plan(
            "string GREATEST/LEAST requires Utf8 arguments".to_string(),
        ));
    }
    Ok(Arc::new(FlinkStringExtremumExpr {
        arguments,
        greatest,
    }))
}

#[cfg(test)]
mod tests {
    use super::choose;

    #[test]
    fn compares_java_utf16_code_units() {
        assert!(choose("\u{e000}", "\u{10000}", true));
        assert!(choose("\u{10000}", "\u{e000}", false));
    }
}
