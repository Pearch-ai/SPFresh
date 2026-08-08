#include "SPFresh/tpsql_bridge/spfresh_c_api.h"

#include "AnnService/inc/Core/Common/TpsqlMemoryLog.h"
#include "AnnService/inc/Core/SPANN/Index.h"
#include "AnnService/inc/Core/SearchQuery.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>

struct tpsql_spfresh_index {
    uint32_t dimension;
    tpsql_spfresh_value_type value_type;
    tpsql_spfresh_distance distance;
    uint32_t build_threads;
    uint32_t ssd_batches;
    uint32_t head_vector_count;
    uint32_t search_internal_result_num;
    bool load_all_vectors;
    std::string index_dir;
    std::shared_ptr<SPTAG::VectorIndex> index;
    std::string last_error;
};

namespace {

SPTAG::VectorValueType native_value_type(tpsql_spfresh_value_type value_type)
{
    switch (value_type) {
    case TPSQL_SPFRESH_FLOAT32:
        return SPTAG::VectorValueType::Float;
    case TPSQL_SPFRESH_INT8:
        return SPTAG::VectorValueType::Int8;
    }
    return SPTAG::VectorValueType::Undefined;
}

const char* value_type_name(tpsql_spfresh_value_type value_type)
{
    switch (value_type) {
    case TPSQL_SPFRESH_FLOAT32:
        return "Float";
    case TPSQL_SPFRESH_INT8:
        return "Int8";
    }
    return "Undefined";
}

SPTAG::DistCalcMethod native_distance(tpsql_spfresh_distance distance)
{
    switch (distance) {
    case TPSQL_SPFRESH_L2:
        return SPTAG::DistCalcMethod::L2;
    case TPSQL_SPFRESH_DOT_PRODUCT:
        return SPTAG::DistCalcMethod::InnerProduct;
    }
    return SPTAG::DistCalcMethod::Undefined;
}

const char* distance_name(tpsql_spfresh_distance distance)
{
    switch (distance) {
    case TPSQL_SPFRESH_L2:
        return "L2";
    case TPSQL_SPFRESH_DOT_PRODUCT:
        return "InnerProduct";
    }
    return "Undefined";
}

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
    uint32_t dimension,
    tpsql_spfresh_distance distance,
    uint32_t build_threads,
    bool rebuild_ssd_only,
    const char* vector_paths,
    uint32_t vector_count,
    uint32_t ssd_batches,
    uint32_t head_vector_count,
    uint32_t search_internal_result_num,
    bool load_all_vectors)
{
    const std::string build_threads_value = std::to_string(build_threads);
    const std::string dimension_value = std::to_string(dimension);
    const std::string vector_count_value = std::to_string(vector_count);
    const std::string ssd_batches_value = std::to_string(ssd_batches);
    const std::string search_internal_result_num_value =
        std::to_string(search_internal_result_num);
    const std::string head_vector_count_value =
        std::to_string(std::min(head_vector_count, vector_count));
    const char* build_head = rebuild_ssd_only ? "false" : "true";
    const char* load_all_vectors_value = load_all_vectors ? "true" : "false";
    const bool has_head_vector_target = head_vector_count > 0;
    const char* select_threshold_value = has_head_vector_target ? "0" : "6";
    const char* split_factor_value = has_head_vector_target ? "0" : "5";
    const char* split_threshold_value = has_head_vector_target ? "0" : "25";
    const auto value_type = index->GetVectorValueType();
    const char* value_type_value = value_type == SPTAG::VectorValueType::Int8 ? "Int8" : "Float";
    return set_parameter(index, "IndexAlgoType", "BKT", "Base")
        && set_parameter(index, "ValueType", value_type_value, "Base")
        && set_parameter(index, "DistCalcMethod", distance_name(distance), "Base")
        && set_parameter(index, "Dim", dimension_value.c_str(), "Base")
        && set_parameter(index, "VectorPath", vector_paths == nullptr ? "" : vector_paths, "Base")
        && set_parameter(index, "VectorType", "DEFAULT", "Base")
        && set_parameter(index, "VectorSize", vector_count_value.c_str(), "Base")
        && set_parameter(index, "IndexDirectory", index_dir.c_str(), "Base")
        && set_parameter(index, "isExecute", build_head, "SelectHead")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "SelectHead")
        && set_parameter(index, "Ratio", "0.2", "SelectHead")
        && set_parameter(index, "Count", head_vector_count_value.c_str(), "SelectHead")
        && set_parameter(index, "SelectThreshold", select_threshold_value, "SelectHead")
        && set_parameter(index, "SplitFactor", split_factor_value, "SelectHead")
        && set_parameter(index, "SplitThreshold", split_threshold_value, "SelectHead")
        && set_parameter(index, "isExecute", build_head, "BuildHead")
        && set_parameter(index, "RefineIterations", "3", "BuildHead")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "BuildHead")
        && set_parameter(index, "isExecute", "true", "BuildSSDIndex")
        && set_parameter(index, "BuildSsdIndex", "true", "BuildSSDIndex")
        && set_parameter(index, "NumberOfThreads", build_threads_value.c_str(), "BuildSSDIndex")
        && set_parameter(index, "PostingPageLimit", "12", "BuildSSDIndex")
        && set_parameter(index, "SearchPostingPageLimit", "12", "BuildSSDIndex")
        && set_parameter(index, "InternalResultNum", "64", "BuildSSDIndex")
        && set_parameter(index, "SearchInternalResultNum", search_internal_result_num_value.c_str(), "BuildSSDIndex")
        && set_parameter(index, "Batches", ssd_batches_value.c_str(), "BuildSSDIndex")
        && set_parameter(index, "LoadAllVectors", load_all_vectors_value, "BuildSSDIndex");
}

bool configure_runtime_search_spann(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    uint32_t search_internal_result_num)
{
    const std::string search_internal_result_num_value =
        std::to_string(search_internal_result_num);
    return set_parameter(index, "SearchInternalResultNum", search_internal_result_num_value.c_str(), "BuildSSDIndex");
}

template <typename T>
bool ensure_posting_mask_sidecar_typed(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir,
    bool rebuild)
{
    auto spann = std::dynamic_pointer_cast<SPTAG::SPANN::Index<T>>(index);
    if (spann == nullptr || spann->GetDiskIndex() == nullptr) return true;

    auto disk_index = spann->GetDiskIndex();
    if (rebuild) {
        return disk_index->BuildPostingMaskSidecar(index_dir)
            && disk_index->LoadPostingMaskSidecar(index_dir);
    }
    if (disk_index->LoadPostingMaskSidecar(index_dir)) return true;
    const auto sidecar_path =
        (std::filesystem::path(index_dir) / "SPTAGFullList.bin.tpsql_mask_sidecar").string();
    const auto probe_path = sidecar_path + ".write_probe";
    {
        std::ofstream probe(probe_path, std::ios::binary | std::ios::trunc);
        if (!probe.is_open()) {
            std::fprintf(
                stderr,
                "ERROR: SPFresh posting mask sidecar is required but missing, stale, or unreadable; refusing to start without masked-posting optimization. Index directory is not writable, so the sidecar cannot be rebuilt here. Build it on a writable indexer first. index_dir=%s sidecar=%s\n",
                index_dir.c_str(),
                sidecar_path.c_str());
            return false;
        }
    }
    std::error_code remove_error;
    std::filesystem::remove(probe_path, remove_error);
    std::fprintf(
        stderr,
        "INFO: SPFresh posting mask sidecar is required but missing, stale, or unreadable; rebuilding before serving. index_dir=%s sidecar=%s\n",
        index_dir.c_str(),
        sidecar_path.c_str());
    return disk_index->BuildPostingMaskSidecar(index_dir)
        && disk_index->LoadPostingMaskSidecar(index_dir);
}

bool ensure_posting_mask_sidecar(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir,
    bool rebuild)
{
    switch (index->GetVectorValueType()) {
    case SPTAG::VectorValueType::Float:
        return ensure_posting_mask_sidecar_typed<float>(index, index_dir, rebuild);
    case SPTAG::VectorValueType::Int8:
        return ensure_posting_mask_sidecar_typed<std::int8_t>(index, index_dir, rebuild);
    default:
        return false;
    }
}

template <typename T>
bool load_or_build_posting_mask_sidecar_typed(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir)
{
    auto spann = std::dynamic_pointer_cast<SPTAG::SPANN::Index<T>>(index);
    if (spann == nullptr || spann->GetDiskIndex() == nullptr) return true;

    auto disk_index = spann->GetDiskIndex();
    if (disk_index->LoadPostingMaskSidecar(index_dir)) return true;
    return disk_index->BuildPostingMaskSidecar(index_dir)
        && disk_index->LoadPostingMaskSidecar(index_dir);
}

bool load_or_build_posting_mask_sidecar(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    const std::string& index_dir)
{
    switch (index->GetVectorValueType()) {
    case SPTAG::VectorValueType::Float:
        return load_or_build_posting_mask_sidecar_typed<float>(index, index_dir);
    case SPTAG::VectorValueType::Int8:
        return load_or_build_posting_mask_sidecar_typed<std::int8_t>(index, index_dir);
    default:
        return false;
    }
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
    if (index->SaveConfig(config_file) != SPTAG::ErrorCode::Success) {
        return false;
    }
    config_file.reset();

    std::ifstream saved(loader_path, std::ios::binary);
    if (!saved.is_open()) return false;
    const std::string contents(
        (std::istreambuf_iterator<char>(saved)),
        std::istreambuf_iterator<char>());
    saved.close();

    std::ofstream loader(loader_path, std::ios::binary | std::ios::trunc);
    if (!loader.is_open()) return false;
    loader << "[Index]\n"
           << "IndexAlgoType=SPANN\n"
           << "ValueType="
           << (index->GetVectorValueType() == SPTAG::VectorValueType::Int8 ? "Int8" : "Float")
           << "\n\n"
           << contents;
    loader.flush();
    return loader.good();
}

tpsql_spfresh_status build_spann(
    tpsql_spfresh_index* handle,
    const void* vectors,
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
            native_value_type(handle->value_type));
        if (index == nullptr) {
            return set_error(handle, TPSQL_SPFRESH_BUILD_FAILED, "failed to create SPFresh index");
        }
        if (!configure_spann(
                index,
                handle->index_dir,
                handle->dimension,
                handle->distance,
                handle->build_threads,
                rebuild_ssd_only,
                nullptr,
                vector_count,
                handle->ssd_batches,
                handle->head_vector_count,
                handle->search_internal_result_num,
                handle->load_all_vectors)) {
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
        if (!ensure_posting_mask_sidecar(index, handle->index_dir, true)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "failed to build SPFresh posting mask sidecar");
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

tpsql_spfresh_status build_spann_from_files(
    tpsql_spfresh_index* handle,
    const char* vector_paths,
    uint32_t vector_count,
    bool rebuild_ssd_only)
{
    if (handle == nullptr || vector_paths == nullptr || vector_paths[0] == '\0'
        || vector_count == 0 || handle->dimension == 0) {
        return set_error(handle, TPSQL_SPFRESH_INVALID_ARGUMENT, "invalid SPFresh file build arguments");
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
            native_value_type(handle->value_type));
        if (index == nullptr) {
            return set_error(handle, TPSQL_SPFRESH_BUILD_FAILED, "failed to create SPFresh index");
        }
        if (!configure_spann(
                index,
                handle->index_dir,
                handle->dimension,
                handle->distance,
                handle->build_threads,
                rebuild_ssd_only,
                vector_paths,
                vector_count,
                handle->ssd_batches,
                handle->head_vector_count,
                handle->search_internal_result_num,
                handle->load_all_vectors)) {
            return set_error(handle, TPSQL_SPFRESH_BUILD_FAILED, "failed to configure SPFresh index");
        }

        SPTAG::ErrorCode status = index->BuildIndex(false);
        if (status != SPTAG::ErrorCode::Success) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh file BuildIndex failed with error code " + std::to_string(static_cast<int>(status)));
        }
        if (!ensure_posting_mask_sidecar(index, handle->index_dir, true)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "failed to build SPFresh posting mask sidecar");
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
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, "unknown SPFresh file build exception");
    }
}

template <typename T>
SPTAG::ErrorCode search_spann(
    const std::shared_ptr<SPTAG::VectorIndex>& index,
    SPTAG::QueryResult& result,
    const uint64_t* filter_bits,
    size_t filter_word_count)
{
    auto spann = std::dynamic_pointer_cast<SPTAG::SPANN::Index<T>>(index);
    if (spann == nullptr) return SPTAG::ErrorCode::Undefined;
    if (filter_bits == nullptr || filter_word_count == 0) {
        return spann->SearchIndex(result);
    }
    return spann->SearchIndex(result, filter_bits, filter_word_count);
}

} // namespace

extern "C" tpsql_spfresh_index* tpsql_spfresh_create(
    uint32_t dimension,
    tpsql_spfresh_value_type value_type,
    tpsql_spfresh_distance distance,
    const char* index_dir,
    uint32_t build_threads,
    uint32_t ssd_batches,
    uint32_t head_vector_count,
    uint32_t search_internal_result_num,
    int load_all_vectors)
{
    if (dimension == 0
        || native_value_type(value_type) == SPTAG::VectorValueType::Undefined
        || native_distance(distance) == SPTAG::DistCalcMethod::Undefined
        || index_dir == nullptr
        || index_dir[0] == '\0'
        || build_threads == 0
        || ssd_batches == 0
        || search_internal_result_num == 0) {
        return nullptr;
    }

    try {
        auto* handle = new tpsql_spfresh_index();
        handle->dimension = dimension;
        handle->value_type = value_type;
        handle->distance = distance;
        handle->build_threads = build_threads;
        handle->ssd_batches = ssd_batches;
        handle->head_vector_count = head_vector_count;
        handle->search_internal_result_num = search_internal_result_num;
        handle->load_all_vectors = load_all_vectors != 0;
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
    const void* vectors,
    uint32_t vector_count)
{
    return build_spann(handle, vectors, vector_count, false);
}

extern "C" tpsql_spfresh_status tpsql_spfresh_rebuild_ssd(
    tpsql_spfresh_index* handle,
    const void* vectors,
    uint32_t vector_count)
{
    return build_spann(handle, vectors, vector_count, true);
}

extern "C" tpsql_spfresh_status tpsql_spfresh_build_from_files(
    tpsql_spfresh_index* handle,
    const char* vector_paths,
    uint32_t vector_count)
{
    return build_spann_from_files(handle, vector_paths, vector_count, false);
}

extern "C" tpsql_spfresh_status tpsql_spfresh_rebuild_ssd_from_files(
    tpsql_spfresh_index* handle,
    const char* vector_paths,
    uint32_t vector_count)
{
    return build_spann_from_files(handle, vector_paths, vector_count, true);
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
        const auto load_start = std::chrono::steady_clock::now();
        SPTAG::TPSQL::LogMemoryEvent(
            "bridge",
            "load_existing_start",
            "VectorIndex::LoadIndex",
            handle->index_dir);
        SPTAG::ErrorCode status = SPTAG::VectorIndex::LoadIndex(handle->index_dir, index);
        const auto load_elapsed_ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - load_start)
                .count());
        SPTAG::TPSQL::LogMemoryEvent(
            "bridge",
            "load_existing_finish",
            "VectorIndex::LoadIndex",
            handle->index_dir,
            SPTAG::TPSQL::UnknownBytes,
            SPTAG::TPSQL::UnknownBytes,
            load_elapsed_ms);
        if (status != SPTAG::ErrorCode::Success || index == nullptr) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh LoadIndex failed with error code " + std::to_string(static_cast<int>(status)));
        }
        if (index->GetVectorValueType() != native_value_type(handle->value_type)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh loaded value type does not match configured "
                    + std::string(value_type_name(handle->value_type)));
        }
        if (index->GetDistCalcMethod() != native_distance(handle->distance)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh loaded distance does not match configured "
                    + std::string(distance_name(handle->distance)));
        }
        if (!configure_runtime_search_spann(index, handle->search_internal_result_num)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "failed to configure SPFresh runtime search parameters");
        }
        if (!ensure_posting_mask_sidecar(index, handle->index_dir, false)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "SPFresh posting mask sidecar is required but missing, stale, or unreadable; run sidecar build on a writable indexer before starting search");
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

extern "C" tpsql_spfresh_status tpsql_spfresh_ensure_posting_mask_sidecar(tpsql_spfresh_index* handle)
{
    if (handle == nullptr || handle->index == nullptr) {
        return set_error(handle, TPSQL_SPFRESH_INVALID_ARGUMENT, "invalid SPFresh sidecar arguments");
    }

    try {
        if (!load_or_build_posting_mask_sidecar(handle->index, handle->index_dir)) {
            return set_error(
                handle,
                TPSQL_SPFRESH_BUILD_FAILED,
                "failed to load or build SPFresh posting mask sidecar");
        }
        handle->last_error.clear();
        return TPSQL_SPFRESH_OK;
    } catch (const std::exception& error) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, error.what());
    } catch (...) {
        return set_error(handle, TPSQL_SPFRESH_EXCEPTION, "unknown SPFresh sidecar exception");
    }
}

extern "C" tpsql_spfresh_status tpsql_spfresh_search(
    tpsql_spfresh_index* handle,
    const void* query,
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
        switch (handle->value_type) {
        case TPSQL_SPFRESH_FLOAT32:
            status = search_spann<float>(
                handle->index, result, filter_bits, filter_word_count);
            break;
        case TPSQL_SPFRESH_INT8:
            status = search_spann<std::int8_t>(
                handle->index, result, filter_bits, filter_word_count);
            break;
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
