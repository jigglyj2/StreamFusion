// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::PAGE_ROWS;

/// Keep row-presence directories compressed until a bounded payload-read chunk requests IDs.
/// Old page directories remain explicit; neither representation copies an ID vector on iteration.
#[derive(Clone, Default)]
pub(in super::super) struct EntryIds {
    explicit: Vec<u64>,
    bitmaps: Vec<(u64, u64)>,
    count: usize,
}

impl EntryIds {
    pub(in super::super) fn with_bitmap_capacity(capacity: usize) -> Self {
        Self {
            bitmaps: Vec::with_capacity(capacity),
            ..Default::default()
        }
    }

    /// Used while reading an ordered legacy page directory; its maximum capacity is admitted
    /// from the legacy page count before payload decoding starts.
    pub(in super::super) fn append(&mut self, id: u64) {
        let page = id / PAGE_ROWS;
        let bit = 1 << (id % PAGE_ROWS);
        if let Some((previous, bits)) = self
            .bitmaps
            .last_mut()
            .filter(|(previous, _)| *previous == page)
        {
            let _ = previous;
            assert_eq!(*bits & bit, 0);
            *bits |= bit;
        } else {
            assert!(self
                .bitmaps
                .last()
                .is_none_or(|(previous, _)| *previous < page));
            self.bitmaps.push((page, bit));
        }
        self.count += 1;
    }

    pub(in super::super) fn contains(&self, id: u64) -> bool {
        self.explicit.binary_search(&id).is_ok()
            || self
                .bitmaps
                .binary_search_by_key(&(id / PAGE_ROWS), |(page, _)| *page)
                .is_ok_and(|index| self.bitmaps[index].1 & (1 << (id % PAGE_ROWS)) != 0)
    }

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
        self.bitmaps.iter().filter(|(_, bits)| *bits != 0).count()
            + self
                .explicit
                .chunk_by(|a, b| a / PAGE_ROWS == b / PAGE_ROWS)
                .count()
    }

    pub(in super::super) fn bitmaps(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.bitmaps
            .iter()
            .copied()
            .filter(|(_, bits)| *bits != 0)
            .chain(
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

    /// A retraction changes one bitmap without shifting a hot key's entire directory.
    pub(in super::super) fn remove(&mut self, id: u64) -> bool {
        if let Ok(index) = self.explicit.binary_search(&id) {
            self.explicit.remove(index);
            self.count -= 1;
            return true;
        }
        if let Ok(index) = self
            .bitmaps
            .binary_search_by_key(&(id / PAGE_ROWS), |(page, _)| *page)
        {
            let mask = 1 << (id % PAGE_ROWS);
            if self.bitmaps[index].1 & mask != 0 {
                self.bitmaps[index].1 &= !mask;
                self.count -= 1;
                return true;
            }
        }
        false
    }

    /// Current directories are cloned from the original and only remove entries. Skip unchanged
    /// directories, then compare bitmaps and expand only removals, not retained identities.
    pub(in super::super) fn removed_from<'a>(
        &'a self,
        original: &'a Self,
    ) -> impl Iterator<Item = u64> + 'a {
        let mut current = self.bitmaps().peekable();
        let pages = if self.count == original.count {
            0
        } else {
            usize::MAX
        };
        original
            .bitmaps()
            .take(pages)
            .flat_map(move |(page, bits)| {
                while current.peek().is_some_and(|(next, _)| *next < page) {
                    current.next();
                }
                let retained = if current.peek().is_some_and(|(next, _)| *next == page) {
                    current.next().unwrap().1
                } else {
                    0
                };
                expand((page, bits & !retained))
            })
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
