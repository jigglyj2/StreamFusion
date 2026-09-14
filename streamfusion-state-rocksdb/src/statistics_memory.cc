// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

// StreamFusion-owned admission metadata, compiled against the same unmodified RocksDB headers.
// No database, statistics factory, or upstream binding is replaced or extended here.
#include <cstddef>
#include <limits>
#include <thread>
#include "monitoring/statistics_impl.h"

extern "C" size_t streamfusion_rocksdb_statistics_memory_required() noexcept {
  using namespace ROCKSDB_NAMESPACE;
  // CoreLocalArray allocates at least eight shards, rounded to the next power of two.
  const auto cpus = std::thread::hardware_concurrency();
  size_t shards = 8;
  while (shards < cpus) {
    if (shards > std::numeric_limits<size_t>::max() / 2) return 0;
    shards *= 2;
  }
  // StatisticsImpl::StatisticsData contains these two fixed arrays. Include a full cache
  // line beyond normal rounding to cover both aligned-new and explicit-padding builds.
  constexpr size_t members = INTERNAL_TICKER_ENUM_MAX * sizeof(std::atomic_uint_fast64_t)
      + INTERNAL_HISTOGRAM_ENUM_MAX * sizeof(HistogramImpl);
  constexpr size_t per_shard = (members + 2 * CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE * CACHE_LINE_SIZE;
  // Coarse headroom covers the statistics object, option owners, allocator bookkeeping and
  // bounded registration metadata. This is one shared reservation, not an allocation ledger.
  constexpr size_t overhead = 64 * 1024;
  if (shards > (std::numeric_limits<size_t>::max() - overhead) / per_shard) return 0;
  return shards * per_shard + overhead;
}
