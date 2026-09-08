// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
#include "cache_priority.h"
#include "rocksdb/db/c.cc"

void rocksdb_lru_cache_options_set_high_pri_pool_ratio(
    rocksdb_lru_cache_options_t* options, double ratio) {
  options->rep.high_pri_pool_ratio = ratio;
}
