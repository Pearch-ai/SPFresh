#include "SPFresh/tpsql_bridge/spfresh_c_api.h"

#include "AnnService/inc/Core/SPANN/Index.h"
#include "AnnService/inc/Core/SearchQuery.h"

#include <algorithm>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>

struct tpsql_spfresh_index {
    uint32_t dimension;
    uint32_t build_threads;
    std::string index_dir;
    std::shared_ptr<SPTAG::VectorIndex> index;
    std::string last_error;
};

namespace {

tpsql_spfresh_status set_error(
    tpsql_spfresh_index* handle,
    tpsql_spfresh_status status,
    const std::string& message)
{
    if (handle != nullptr) {
        handle->last_error = message;
    }
    return status;
}

bool set_parameter(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const char* key,
    const char* value,
    const char* section)
{
    return index->SetParameter(key, value, section) == SPTAG::ErrorCode::Success;
}

bool configure_spann(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir,
    uint32_t build_threads,
    bool rebuild_ssd_only)
{
    const std::string build_threads_value = std::to_string(build_threads);
    const char* build_head = rebuild_ssd_only ? "false" : "true";
    return set_parameter(index, "IndexAlgoType", "BKT", "Base")
        && set_parameter(index, "DistCalcMethod", "L2", "Base")
        && set_parameter(index, "IndexDirectory", index_dir.c_str(), "Base")
        && set_parameter(index, "isExecute", build_head, "SelectHead")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "SelectHead")
        && set_parameter(index, "Ratio", "0.2", "SelectHead")
        && set_parameter(index, "isExecute", build_head, "BuildHead")
        && set_parameter(index, "RefineIterations", "3", "BuildHead")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "BuildHead")
        && set_parameter(index, "isExecute", "true", "BuildSSDIndex")
        && set_parameter(index, "BuildSsdIndex", "true", "BuildSSDIndex")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "BuildSSDIndex")
        && set_parameter(index, "PostingPageLimit", "12", "BuildSSDIndex")
        && set_parameter(index, "SearchPostingPageLimit", "12", "BuildSSDIndex")
        && set_parameter(index, "InternalResultNum", "64", "BuildSSDIndex")
        && set_parameter(index, "SearchInternalResultNum", "64", "BuildSSDIndex");
}

bool write_loader_config(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir)
{
    std::filesystem::create_directories(index_dir);
    auto config_file = SPTAG::f_createIO();
    const auto loader_path =
        (std::filesystem::path(index_dir) / "indexloader.ini").string();
    if (config_file == nullptr
        || !config_file->Initialize(loader_path.c_str(), std::ios::out)) {
        return false;
    }
    return index->SaveConfig(config_file) == SPTAG::ErrorCode::Success;
}

tpsql_spfresh_status build_spann(
    tpsql_spfresh_index* handle,
    const float* vectors,
    uint32_t vector_count,
    bool rebuild_ssd_only)
{
    if (handle == nullptr || vectors == nullptr || vector_count == 0 || handle->dimension == 0) {
        return set_error(handle, TPSQL_SPFRESH_INVALID_ARGUMENT, "invalid SPFresh build arguments");
    }

    try {
        std::filesystem::create_directories(handle->index_dir);
        if (rebuild_ssd_only) {
            const auto head_loader =
                std::filesystem::path(handle->index_dir) / "HeadIndex" / "indexloader.ini";
            if (!std::filesystem::exists(head_loader)) {
                return set_error(
                    handle,
                    TPSQL_SPFRESH_BUILD_FAILED,
                    "cannot continue SPFresh rebuild: missing HeadIndex/indexloader.ini");
            }

            const auto ssd_file =
                std::filesystem::path(handle->index_dir) / "SPTAGFullList.bin";
            std::error_code remove_error;
            std::filesystem::remove(ssd_file, remove_error);
            if (remove_error) {
                return set_error(
                    handle,
                    TPSQL_SPFRESH_BUILD_FAILED,
                    "cannot remove incomplete SPFresh SSD index file: " + remove_error.message());
            }
        }

        auto index = SPTAG::VectorIndex::CreateInstance(
            SPTAG::IndexAlgoType::SPANN,
            SPTAG::VectorValueType::Float);
        if (index == nullptr) {
            return set_error(handle, TPSQL_SPFRESH_BUILD_FAILED, "failed to create SPFresh index");
        }
        if (!configure_spann(index, handle->index_dir, handle->build_threads, rebuild_ssd_only)) {
            return set_error(handle, TPSQL_SPFRESH_BUILD_FAILED, "failed to configure SPFresh index");
        }

        SPTAG::ErrorCode status = index->BuildIndex(
            vectors,
            static_cast<SPTAG::SizeType>(vector_count),
            static_cast<SPTAG::DimensionType>(handle->dimension),
            false,
            false);
        if (status != SPTAG::ErrorCode::Success) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh BuildIndex failed with error code " + std::to_string(static_cast<int>(status)));
        }
        if (!write_loader_config(index, handle->index_dir)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "failed to write SPFresh indexloader.ini");
        }

        handle->index = std::move(index);
        handle->last_error.clear();
        return TPSQL_SPFRESH_OK;
    } catch (const std::exception& error) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, error.what());
    } catch (...) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, "unknown SPFresh build exception");
    }
}

} // namespace

extern "C" tpsql_spfresh_index* tpsql_spfresh_create(
    uint32_t dimension,
    const char* index_dir,
    uint32_t build_threads)
{
    if (dimension == 0 || index_dir == nullptr || index_dir[0] == '\0' || build_threads == 0) {
        return nullptr;
    }

    try {
        auto* handle = new tpsql_spfresh_index();
        handle->dimension = dimension;
        handle->build_threads = build_threads;
        handle->index_dir = index_dir;
        return handle;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void tpsql_spfresh_destroy(tpsql_spfresh_index* index)
{
    // SPANN indexes built through the in-memory API currently crash during
    // native teardown in this embedding. Keep process stability for the first
    // integration; rebuild lifecycle should be revisited before production use.
    (void)index;
}

extern "C" tpsql_spfresh_status tpsql_spfresh_build(
    tpsql_spfresh_index* handle,
    const float* vectors,
    uint32_t vector_count)
{
    return build_spann(handle, vectors, vector_count, false);
}

extern "C" tpsql_spfresh_status tpsql_spfresh_rebuild_ssd(
    tpsql_spfresh_index* handle,
    const float* vectors,
    uint32_t vector_count)
{
    return build_spann(handle, vectors, vector_count, true);
}

extern "C" tpsql_spfresh_status tpsql_spfresh_load_existing(tpsql_spfresh_index* handle)
{
    if (handle == nullptr || handle->index_dir.empty()) {
        return set_error(handle, TPSQL_SPFRESH_INVALID_ARGUMENT, "invalid SPFresh load arguments");
    }

    try {
        const auto loader_path =
            std::filesystem::path(handle->index_dir) / "indexloader.ini";
        if (!std::filesystem::exists(loader_path)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "cannot load SPFresh index: missing indexloader.ini");
        }

        std::shared_ptr<SPTAG::VectorIndex> index;
        SPTAG::ErrorCode status = SPTAG::VectorIndex::LoadIndex(handle->index_dir, index);
        if (status != SPTAG::ErrorCode::Success || index == nullptr) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh LoadIndex failed with error code " + std::to_string(static_cast<int>(status)));
        }
        handle->dimension = static_cast<uint32_t>(index->GetFeatureDim());
        handle->index = std::move(index);
        handle->last_error.clear();
        return TPSQL_SPFRESH_OK;
    } catch (const std::exception& error) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, error.what());
    } catch (...) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, "unknown SPFresh load exception");
    }
}

extern "C" tpsql_spfresh_status tpsql_spfresh_search(
    tpsql_spfresh_index* handle,
    const float* query,
    uint32_t top_k,
    const uint64_t* filter_bits,
    size_t filter_word_count,
    tpsql_spfresh_hit* hits,
    uint32_t* in_out_hit_count)
{
    if (handle == nullptr
        || handle->index == nullptr
        || query == nullptr
        || in_out_hit_count == nullptr
        || (*in_out_hit_count > 0 && hits == nullptr)) {
        return set_error(handle, TPSQL_SPFRESH_INVALID_ARGUMENT, "invalid SPFresh search arguments");
    }

    uint32_t capacity = *in_out_hit_count;
    *in_out_hit_count = 0;
    if (top_k == 0 || capacity == 0) {
        return TPSQL_SPFRESH_OK;
    }

    int result_count = static_cast<int>(std::min(top_k, capacity));
    try {
        SPTAG::QueryResult result(query, result_count, false);
        SPTAG::ErrorCode status = SPTAG::ErrorCode::Undefined;
        auto spann = std::dynamic_pointer_cast<SPTAG::SPANN::Index<float>>(handle->index);
        if (spann == nullptr) {
            return set_error(handle, TPSQL_SPFRESH_SEARCH_FAILED, "SPFresh index has unexpected runtime type");
        }
        if (filter_bits == nullptr || filter_word_count == 0) {
            status = spann->SearchIndex(result);
        } else {
            status = spann->SearchIndex(result, filter_bits, filter_word_count);
        }
        if (status != SPTAG::ErrorCode::Success) {
            return set_error(
                handle,
                TPSQL_SPFRESH_SEARCH_FAILED,
                "SPFresh SearchIndex failed with error code " + std::to_string(static_cast<int>(status)));
        }

        for (int idx = 0; idx < result_count; ++idx) {
            const auto* item = result.GetResult(idx);
            if (item == nullptr || item->VID < 0 || item->Dist >= SPTAG::MaxDist) {
                continue;
            }
            hits[*in_out_hit_count] = tpsql_spfresh_hit {
                static_cast<uint32_t>(item->VID),
                item->Dist,
            };
            *in_out_hit_count += 1;
        }
        handle->last_error.clear();
        return TPSQL_SPFRESH_OK;
    } catch (const std::exception& error) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, error.what());
    } catch (...) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, "unknown SPFresh search exception");
    }
}

extern "C" const char* tpsql_spfresh_last_error(const tpsql_spfresh_index* index)
{
    if (index == nullptr) {
        return "null SPFresh index";
    }
    return index->last_error.c_str();
}
