// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::PAGE_ROWS;

/// Keep row-presence directories compressed until a bounded payload-read chunk requests IDs.
/// Old page directories remain explicit; neither representation copies an ID vector on iteration.
#[derive(Default)]
pub(in super::super) struct EntryIds {
    explicit: Vec<u64>,
    bitmaps: Vec<(u64, u64)>,
    count: usize,
}

impl EntryIds {
    pub(in super::super) fn explicit(ids: Vec<u64>) -> Self {
        Self {
            count: ids.len(),
            explicit: ids,
            bitmaps: Vec::new(),
        }
    }

    pub(super) fn from_bitmaps(pages: Vec<(u64, u64)>) -> Self {
        // The persisted framing/counts and maximum identities were validated before construction.
        Self {
            count: pages
                .iter()
                .map(|(_, bits)| bits.count_ones() as usize)
                .sum(),
            explicit: Vec::new(),
            bitmaps: pages,
        }
    }

    pub(in super::super) fn len(&self) -> usize {
        self.count
    }

    pub(in super::super) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(in super::super) fn bitmap_count(&self) -> usize {
        self.bitmaps.len()
            + self
                .explicit
                .chunk_by(|a, b| a / PAGE_ROWS == b / PAGE_ROWS)
                .count()
    }

    pub(in super::super) fn bitmaps(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.bitmaps.iter().copied().chain(
            self.explicit
                .chunk_by(|a, b| a / PAGE_ROWS == b / PAGE_ROWS)
                .map(|ids| {
                    (
                        ids[0] / PAGE_ROWS,
                        ids.iter()
                            .fold(0, |bits, id| bits | (1 << (id % PAGE_ROWS))),
                    )
                }),
        )
    }

    pub(in super::super) fn allocated_bytes(&self) -> usize {
        self.explicit
            .capacity()
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_add(
                self.bitmaps
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u64, u64)>()),
            )
    }

    pub(in super::super) fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.explicit
            .iter()
            .copied()
            .chain(self.bitmaps.iter().copied().flat_map(expand))
    }
}

pub(in super::super) struct BitmapBits {
    page: u64,
    bits: u64,
}

fn expand((page, bits): (u64, u64)) -> BitmapBits {
    BitmapBits { page, bits }
}

impl Iterator for BitmapBits {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.bits == 0 {
            return None;
        }
        let id = self.page * PAGE_ROWS + u64::from(self.bits.trailing_zeros());
        self.bits &= self.bits - 1;
        Some(id)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.bits.count_ones() as usize;
        (count, Some(count))
    }
}

impl ExactSizeIterator for BitmapBits {}

impl IntoIterator for EntryIds {
    type Item = u64;
    type IntoIter = std::iter::Chain<
        std::vec::IntoIter<u64>,
        std::iter::FlatMap<
            std::vec::IntoIter<(u64, u64)>,
            BitmapBits,
            fn((u64, u64)) -> BitmapBits,
        >,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.explicit.into_iter().chain(
            self.bitmaps
                .into_iter()
                .flat_map(expand as fn((u64, u64)) -> BitmapBits),
        )
    }
}
