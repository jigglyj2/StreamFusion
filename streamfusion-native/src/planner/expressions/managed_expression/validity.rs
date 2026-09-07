// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;

/// Borrow nested arrays directly: to_data() would allocate descriptor vectors before
/// admission. Field/element extraction may forward a large child's validity bitmap.
pub(super) fn retained_bytes(array: &dyn Array) -> Option<usize> {
    let own = array.nulls().map_or(0, |nulls| {
        nulls.buffer().capacity().max(nulls.buffer().len())
    });
    let children = match array.data_type() {
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0usize, |bytes, child| {
                bytes.checked_add(retained_bytes(child.as_ref())?)
            })?,
        DataType::List(_) => retained_bytes(array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => retained_bytes(array.as_list::<i64>().values().as_ref())?,
        DataType::FixedSizeList(_, _) => {
            retained_bytes(array.as_fixed_size_list().values().as_ref())?
        }
        DataType::ListView(_) => retained_bytes(array.as_list_view::<i32>().values().as_ref())?,
        DataType::LargeListView(_) => {
            retained_bytes(array.as_list_view::<i64>().values().as_ref())?
        }
        DataType::Map(_, _) => retained_bytes(array.as_map().entries())?,
        DataType::Dictionary(_, _) => retained_bytes(array.as_any_dictionary().values().as_ref())?,
        // These are not Flink SQL boundary types; retain conservative accounting if
        // a native child nevertheless forwards their storage to a fixed-width result.
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => array.get_array_memory_size(),
        _ => 0,
    };
    own.checked_add(children)
}
