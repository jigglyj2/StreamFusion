// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

// Mirrors Flink BinarySegmentUtils.hashByWords (also implemented by paimon-rust)
// followed by Flink MathUtils.murmurHash (also implemented by fluss-rust).
const C1: u32 = 0xcc9e2d51;
const C2: u32 = 0x1b873593;

pub fn binary_row_hash(bytes: &[u8]) -> i32 {
    assert!(bytes.len().is_multiple_of(4));
    let mut hash = 42_u32;
    for chunk in bytes.chunks_exact(4) {
        let word = u32::from_le_bytes(chunk.try_into().expect("four-byte chunk"));
        hash = mix_hash(hash, mix_word(word));
    }
    finalize(hash ^ bytes.len() as u32) as i32
}

pub fn flink_murmur_hash(value: i32) -> i32 {
    let mixed = mix_hash(0, mix_word(value as u32)) ^ 4;
    let result = finalize(mixed) as i32;
    if result >= 0 {
        result
    } else if result != i32::MIN {
        -result
    } else {
        0
    }
}

pub fn assign_key_group(binary_row: &[u8], max_parallelism: u32) -> u32 {
    assert!(max_parallelism > 0 && max_parallelism <= 32_768);
    flink_murmur_hash(binary_row_hash(binary_row)) as u32 % max_parallelism
}

fn mix_word(mut value: u32) -> u32 {
    value = value.wrapping_mul(C1);
    value = value.rotate_left(15);
    value.wrapping_mul(C2)
}

fn mix_hash(hash: u32, word: u32) -> u32 {
    (hash ^ word)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe6546b64)
}

fn finalize(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x85ebca6b);
    value ^= value >> 13;
    value = value.wrapping_mul(0xc2b2ae35);
    value ^ (value >> 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_fluss_flink_integer_murmur_vectors() {
        assert_eq!(flink_murmur_hash(0), 0x2362_f9de);
        assert_eq!(flink_murmur_hash(42), 0x43a4_6e1d);
        assert_eq!(flink_murmur_hash(-77), 0x2eeb_27de);
    }

    #[test]
    fn hashes_word_aligned_binary_rows_with_paimons_seed() {
        assert_eq!(binary_row_hash(&[]), 142_593_372);
        assert_eq!(binary_row_hash(&1_i32.to_le_bytes()), -559_580_957);
        assert_eq!(assign_key_group(&1_i32.to_le_bytes(), 128), 2);
    }
}
