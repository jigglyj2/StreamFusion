// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::{validate_key_group_snapshot, SnapshotError, SnapshotInput};

/// Validate the entire immutable frame before exposing borrowed entries. Semantic restore checks
/// can inspect headers without copying a second whole snapshot or allocating from encoded counts.
pub fn key_group_snapshot_entries<'a>(
    group: u32,
    bytes: &'a [u8],
) -> Result<impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])> + 'a, SnapshotError> {
    let remaining = validate_key_group_snapshot(group, bytes)?;
    let mut input = SnapshotInput::new(bytes);
    input.offset = 16;
    Ok(Entries { input, remaining })
}

struct Entries<'a> {
    input: SnapshotInput<'a>,
    remaining: usize,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let key_len = self.input.read_u32("key length").expect("validated frame") as usize;
        let key = self
            .input
            .read_exact(key_len, "key")
            .expect("validated frame");
        let value_len = self
            .input
            .read_u32("value length")
            .expect("validated frame") as usize;
        let value = self
            .input
            .read_exact(value_len, "value")
            .expect("validated frame");
        self.remaining -= 1;
        Some((key, value))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for Entries<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode_key_group_snapshot;
    #[test]
    fn borrowed_entries_preserve_values_and_validate_the_entire_frame_before_visitation() {
        let entries = [
            (b"".as_slice(), b"abc".as_slice()),
            (b"key".as_slice(), b"".as_slice()),
        ];
        let mut bytes = encode_key_group_snapshot(3, entries.into_iter()).unwrap();
        let actual = key_group_snapshot_entries(3, &bytes)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(actual, entries);
        let address = bytes.as_ptr() as usize;
        for (key, value) in actual {
            for item in [key, value] {
                assert!((address..=address + bytes.len()).contains(&(item.as_ptr() as usize)));
            }
        }
        for end in 0..bytes.len() {
            assert!(key_group_snapshot_entries(3, &bytes[..end]).is_err());
        }
        assert!(key_group_snapshot_entries(4, &bytes).is_err());
        bytes.push(0);
        assert!(key_group_snapshot_entries(3, &bytes).is_err());
    }
}
