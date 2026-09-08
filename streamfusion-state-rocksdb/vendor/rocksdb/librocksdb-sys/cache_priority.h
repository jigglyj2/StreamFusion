// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
#pragma once
#include <rocksdb/c.h>
#ifdef __cplusplus
extern "C" {
#endif
ROCKSDB_LIBRARY_API void rocksdb_lru_cache_options_set_high_pri_pool_ratio(
    rocksdb_lru_cache_options_t* options, double ratio);
#ifdef __cplusplus
}
#endif
