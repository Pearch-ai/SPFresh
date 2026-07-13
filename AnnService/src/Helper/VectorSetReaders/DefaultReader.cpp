// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/VectorSetReaders/DefaultReader.h"
#include "inc/Core/Common/TpsqlMemoryLog.h"
#include "inc/Helper/CommonHelper.h"

#include <algorithm>
#include <chrono>

using namespace SPTAG;
using namespace SPTAG::Helper;

DefaultVectorReader::DefaultVectorReader(std::shared_ptr<ReaderOptions> p_options)
    : VectorSetReader(p_options)
{
    m_vectorOutput = "";
    m_metadataConentOutput = "";
    m_metadataIndexOutput = "";
}


DefaultVectorReader::~DefaultVectorReader()
{
}


ErrorCode
DefaultVectorReader::LoadFile(const std::string& p_filePaths)
{
    const auto& files = SPTAG::Helper::StrUtils::SplitString(p_filePaths, ",");
    m_vectorOutput = files[0];
    m_vectorOutputs = SPTAG::Helper::StrUtils::SplitString(m_vectorOutput, ";");
    m_vectorOutputs.erase(
        std::remove_if(
            m_vectorOutputs.begin(),
            m_vectorOutputs.end(),
            [](const std::string& path) { return path.empty(); }),
        m_vectorOutputs.end());
    if (files.size() >= 3) {
        m_metadataConentOutput = files[1];
        m_metadataIndexOutput = files[2];
    }
    return ErrorCode::Success;
}


std::shared_ptr<VectorSet>
DefaultVectorReader::GetVectorSet(SizeType start, SizeType end) const
{
    if (m_options->m_loadAllVectors && m_cachedVectorSet != nullptr && start == 0 &&
        (end < 0 || end == m_cachedVectorSet->Count())) {
        return m_cachedVectorSet;
    }

    std::vector<std::string> vectorFiles =
        m_vectorOutputs.empty() ? std::vector<std::string>{m_vectorOutput} : m_vectorOutputs;

    struct Source {
        std::string path;
        SizeType row;
        DimensionType col;
        SizeType start;
    };

    std::vector<Source> sources;
    sources.reserve(vectorFiles.size());
    SizeType totalRows = 0;
    DimensionType col = 0;
    for (const auto& vectorFile : vectorFiles) {
        auto ptr = f_createIO();
        if (ptr == nullptr || !ptr->Initialize(vectorFile.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read file %s.\n", vectorFile.c_str());
            throw std::runtime_error("Failed read file");
        }

        SizeType row;
        DimensionType fileCol;
        if (ptr->ReadBinary(sizeof(SizeType), (char*)&row) != sizeof(SizeType)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
            throw std::runtime_error("Failed read file");
        }
        if (ptr->ReadBinary(sizeof(DimensionType), (char*)&fileCol) != sizeof(DimensionType)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
            throw std::runtime_error("Failed read file");
        }
        if (col == 0) {
            col = fileCol;
        } else if (col != fileCol) {
            LOG(Helper::LogLevel::LL_Error, "Vector file %s has dimension %d, expected %d.\n", vectorFile.c_str(), fileCol, col);
            throw std::runtime_error("Vector dimension mismatch");
        }
        sources.push_back(Source{vectorFile, row, fileCol, totalRows});
        totalRows += row;
    }
    if (start > totalRows) start = totalRows;
    if (end < 0 || end > totalRows) end = totalRows;
    const bool cacheFullRange = m_options->m_loadAllVectors && start == 0 && end == totalRows;
    std::uint64_t totalRecordVectorBytes = ((std::uint64_t)GetValueTypeSize(m_options->m_inputValueType)) * (end - start) * col;
    ByteArray vectorSet;
    if (totalRecordVectorBytes > 0) {
        const auto allocStart = std::chrono::steady_clock::now();
        SPTAG::TPSQL::LogMemoryEvent(
            "sptag_vector_reader",
            "vector_buffer_alloc_start",
            "DefaultVectorReader::GetVectorSet",
            std::string(),
            SPTAG::TPSQL::UnknownBytes,
            totalRecordVectorBytes,
            SPTAG::TPSQL::UnknownBytes,
            "source_count=" + std::to_string(sources.size())
                + " start=" + std::to_string(start)
                + " end=" + std::to_string(end)
                + " cache_full_range=" + std::string(cacheFullRange ? "true" : "false"));
        vectorSet = ByteArray::Alloc(totalRecordVectorBytes);
        const auto allocElapsedMs = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - allocStart)
                .count());
        SPTAG::TPSQL::LogMemoryEvent(
            "sptag_vector_reader",
            "vector_buffer_alloc_finish",
            "DefaultVectorReader::GetVectorSet",
            std::string(),
            SPTAG::TPSQL::UnknownBytes,
            totalRecordVectorBytes,
            allocElapsedMs,
            "source_count=" + std::to_string(sources.size())
                + " start=" + std::to_string(start)
                + " end=" + std::to_string(end)
                + " cache_full_range=" + std::string(cacheFullRange ? "true" : "false"));
        char* vecBuf = reinterpret_cast<char*>(vectorSet.Data());
        std::uint64_t copiedBytes = 0;
        const std::uint64_t vectorBytes = ((std::uint64_t)GetValueTypeSize(m_options->m_inputValueType)) * col;
        for (const auto& source : sources) {
            const SizeType sourceStart = source.start;
            const SizeType sourceEnd = source.start + source.row;
            if (sourceEnd <= start || sourceStart >= end) {
                continue;
            }
            const SizeType readStart = std::max(start, sourceStart);
            const SizeType readEnd = std::min(end, sourceEnd);
            const SizeType localStart = readStart - sourceStart;
            const SizeType readRows = readEnd - readStart;
            const std::uint64_t readBytes = vectorBytes * readRows;
            const std::uint64_t offset = vectorBytes * localStart + sizeof(SizeType) + sizeof(DimensionType);
            const std::uint64_t sourceFileBytes = SPTAG::TPSQL::FileSizeBytes(source.path);

            const auto readStartTime = std::chrono::steady_clock::now();
            SPTAG::TPSQL::LogMemoryEvent(
                "sptag_vector_reader",
                "vector_file_heap_read_start",
                "DefaultVectorReader::GetVectorSet",
                source.path,
                sourceFileBytes,
                readBytes,
                SPTAG::TPSQL::UnknownBytes,
                "read_rows=" + std::to_string(readRows)
                    + " read_start=" + std::to_string(readStart)
                    + " read_end=" + std::to_string(readEnd)
                    + " file_offset=" + std::to_string(offset));
            auto ptr = f_createIO();
            if (ptr == nullptr || !ptr->Initialize(source.path.c_str(), std::ios::binary | std::ios::in)) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read file %s.\n", source.path.c_str());
                throw std::runtime_error("Failed read file");
            }
            if (ptr->ReadBinary(readBytes, vecBuf + copiedBytes, offset) != readBytes) {
                LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
                throw std::runtime_error("Failed read file");
            }
            copiedBytes += readBytes;
            const auto readElapsedMs = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - readStartTime)
                    .count());
            SPTAG::TPSQL::LogMemoryEvent(
                "sptag_vector_reader",
                "vector_file_heap_read_finish",
                "DefaultVectorReader::GetVectorSet",
                source.path,
                sourceFileBytes,
                readBytes,
                readElapsedMs,
                "read_rows=" + std::to_string(readRows)
                    + " read_start=" + std::to_string(readStart)
                    + " read_end=" + std::to_string(readEnd)
                    + " file_offset=" + std::to_string(offset));
        }
    }

    LOG(Helper::LogLevel::LL_Info, "Load Vector(%d,%d)\n", end - start, col);
    auto result = std::make_shared<BasicVectorSet>(vectorSet,
                                                  m_options->m_inputValueType,
                                                  col,
                                                  end - start);
    if (cacheFullRange) {
        m_cachedVectorSet = result;
    }
    return result;
}


std::shared_ptr<MetadataSet>
DefaultVectorReader::GetMetadataSet() const
{
    if (fileexists(m_metadataIndexOutput.c_str()) && fileexists(m_metadataConentOutput.c_str()))
        return std::shared_ptr<MetadataSet>(new FileMetadataSet(m_metadataConentOutput, m_metadataIndexOutput));
    return nullptr;
}
