// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use datafusion::error::{DataFusionError, Result};
use streamfusion_state_abi::{decode_key_group_snapshot, encode_key_group_snapshot};

pub(crate) fn encode<'a>(
    key_group: u32,
    entries: impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<Vec<u8>> {
    encode_key_group_snapshot(key_group, entries)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

pub(crate) fn decode(expected_key_group: u32, bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    decode_key_group_snapshot(expected_key_group, bytes)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_versioned_key_group_entries() {
        let entries = vec![(vec![1, 2], vec![3, 4, 5])];
        let encoded = encode(
            7,
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .unwrap();

        assert_eq!(decode(7, &encoded).unwrap(), entries);
        assert!(decode(8, &encoded)
            .unwrap_err()
            .to_string()
            .contains("does not match"));
    }
}
