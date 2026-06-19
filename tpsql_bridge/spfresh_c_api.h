#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tpsql_spfresh_index tpsql_spfresh_index;

typedef struct tpsql_spfresh_hit {
    uint32_t vector_id;
    float distance;
} tpsql_spfresh_hit;

typedef enum tpsql_spfresh_status {
    TPSQL_SPFRESH_OK = 0,
    TPSQL_SPFRESH_INVALID_ARGUMENT = 1,
    TPSQL_SPFRESH_BUILD_FAILED = 2,
    TPSQL_SPFRESH_SEARCH_FAILED = 3,
    TPSQL_SPFRESH_EXCEPTION = 4
} tpsql_spfresh_status;

tpsql_spfresh_index* tpsql_spfresh_create(
    uint32_t dimension,
    const char* index_dir,
    uint32_t build_threads);
void tpsql_spfresh_destroy(tpsql_spfresh_index* index);

tpsql_spfresh_status tpsql_spfresh_build(
    tpsql_spfresh_index* index,
    const float* vectors,
    uint32_t vector_count);

tpsql_spfresh_status tpsql_spfresh_rebuild_ssd(
    tpsql_spfresh_index* index,
    const float* vectors,
    uint32_t vector_count);

tpsql_spfresh_status tpsql_spfresh_load_existing(tpsql_spfresh_index* index);

tpsql_spfresh_status tpsql_spfresh_search(
    tpsql_spfresh_index* index,
    const float* query,
    uint32_t top_k,
    const uint64_t* filter_bits,
    size_t filter_word_count,
    tpsql_spfresh_hit* hits,
    uint32_t* in_out_hit_count);

const char* tpsql_spfresh_last_error(const tpsql_spfresh_index* index);

#ifdef __cplusplus
}
#endif
