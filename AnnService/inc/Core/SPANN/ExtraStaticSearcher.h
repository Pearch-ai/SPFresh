// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_EXTRASTATICSEARCHER_H_
#define _SPTAG_SPANN_EXTRASTATICSEARCHER_H_

#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/AsyncFileReader.h"
#include "IExtraSearcher.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/Common/TpsqlMemoryLog.h"
#include "Compressor.h"

#include <map>
#include <algorithm>
#include <cmath>
#include <climits>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <numeric>
#include <string>

namespace SPTAG
{
    namespace SPANN
    {
        extern std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO;

        struct Selection {
            std::string m_tmpfile;
            size_t m_totalsize;
            size_t m_start;
            size_t m_end;
            std::vector<Edge> m_selections;
            static EdgeCompare g_edgeComparer;

            Selection(size_t totalsize, std::string tmpdir) : m_tmpfile(tmpdir + FolderSep + "selection_tmp"), m_totalsize(totalsize), m_start(0), m_end(totalsize) { remove(m_tmpfile.c_str()); m_selections.resize(totalsize); }

            ErrorCode SaveBatch()
            {
                auto f_out = f_createIO();
                if (f_out == nullptr || !f_out->Initialize(m_tmpfile.c_str(), std::ios::out | std::ios::binary | (fileexists(m_tmpfile.c_str()) ? std::ios::in : 0))) {
                    LOG(Helper::LogLevel::LL_Error, "Cannot open %s to save selection for batching!\n", m_tmpfile.c_str());
                    return ErrorCode::FailedOpenFile;
                }
                if (f_out->WriteBinary(sizeof(Edge) * (m_end - m_start), (const char*)m_selections.data(), sizeof(Edge) * m_start) != sizeof(Edge) * (m_end - m_start)) {
                    LOG(Helper::LogLevel::LL_Error, "Cannot write to %s!\n", m_tmpfile.c_str());
                    return ErrorCode::DiskIOFail;
                }
                std::vector<Edge> batch_selection;
                m_selections.swap(batch_selection);
                m_start = m_end = 0;
                return ErrorCode::Success;
            }

            ErrorCode LoadBatch(size_t start, size_t end)
            {
                auto f_in = f_createIO();
                if (f_in == nullptr || !f_in->Initialize(m_tmpfile.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Cannot open %s to load selection batch!\n", m_tmpfile.c_str());
                    return ErrorCode::FailedOpenFile;
                }

                size_t readsize = end - start;
                m_selections.resize(readsize);
                if (f_in->ReadBinary(readsize * sizeof(Edge), (char*)m_selections.data(), start * sizeof(Edge)) != readsize * sizeof(Edge)) {
                    LOG(Helper::LogLevel::LL_Error, "Cannot read from %s! start:%zu size:%zu\n", m_tmpfile.c_str(), start, readsize);
                    return ErrorCode::DiskIOFail;
                }
                m_start = start;
                m_end = end;
                return ErrorCode::Success;
            }

            size_t lower_bound(SizeType node)
            {
                auto ptr = std::lower_bound(m_selections.begin(), m_selections.end(), node, g_edgeComparer);
                return m_start + (ptr - m_selections.begin());
            }

            Edge& operator[](size_t offset)
            {
                if (offset < m_start || offset >= m_end) {
                    LOG(Helper::LogLevel::LL_Error, "Error read offset in selections:%zu\n", offset);
                }
                return m_selections[offset - m_start];
            }
        };

#define DecompressPosting(){\
        p_postingListFullData = (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer(); \
        if (listInfo->listEleCount != 0) { \
            std::size_t sizePostingListFullData;\
            try {\
                sizePostingListFullData = m_pCompressor->Decompress(buffer + listInfo->pageOffset, listInfo->listTotalBytes, p_postingListFullData, listInfo->listEleCount * m_vectorInfoSize, m_enableDictTraining);\
            }\
            catch (std::runtime_error& err) {\
                LOG(Helper::LogLevel::LL_Error, "Decompress postingList %d  failed! %s, \n", listInfo - m_listInfos.data(), err.what());\
                return;\
            }\
            if (sizePostingListFullData != listInfo->listEleCount * m_vectorInfoSize) {\
                LOG(Helper::LogLevel::LL_Error, "PostingList %d decompressed size not match! %zu, %d, \n", listInfo - m_listInfos.data(), sizePostingListFullData, listInfo->listEleCount * m_vectorInfoSize);\
                return;\
            }\
        }\
}\

#define ProcessPosting() \
        for (int i = 0; i < listInfo->listEleCount; i++) { \
            uint64_t offsetVectorID, offsetVector;\
            (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount);\
            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));\
            if (!p_exWorkSpace->CheckVectorFilter(vectorID)) continue; \
            if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) continue; \
            (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector));\
            auto distance2leaf = p_index->ComputeDistance(queryResults.GetQuantizedTarget(), p_postingListFullData + offsetVector); \
            queryResults.AddPoint(vectorID, distance2leaf); \
        } \

#define ProcessPostingPartial(loadedPageMask) \
        for (int i = 0; i < listInfo->listEleCount; i++) { \
            const std::uint64_t recordStart = static_cast<std::uint64_t>(listInfo->pageOffset) + static_cast<std::uint64_t>(i) * static_cast<std::uint64_t>(m_vectorInfoSize);\
            const std::uint64_t recordEnd = recordStart + static_cast<std::uint64_t>(m_vectorInfoSize) - 1;\
            const std::uint64_t recordPageMask = this->PageMaskForByteRange(recordStart, recordEnd);\
            if ((recordPageMask & loadedPageMask) != recordPageMask) continue;\
            uint64_t offsetVectorID, offsetVector;\
            (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount);\
            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));\
            if (!p_exWorkSpace->CheckVectorFilter(vectorID)) continue; \
            if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) continue; \
            (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector));\
            auto distance2leaf = p_index->ComputeDistance(queryResults.GetQuantizedTarget(), p_postingListFullData + offsetVector); \
            queryResults.AddPoint(vectorID, distance2leaf); \
        } \

        template <typename ValueType>
        class ExtraStaticSearcher : public IExtraSearcher
        {
        private:
            struct PageReadRange
            {
                std::uint16_t firstPage = 0;
                std::uint16_t pageCount = 0;
            };

            struct ListInfo
            {
                std::size_t listTotalBytes = 0;

                int listEleCount = 0;

                std::uint16_t listPageCount = 0;

                std::uint64_t listOffset = 0;

                std::uint16_t pageOffset = 0;
            };

        public:
            ExtraStaticSearcher()
            {
                m_enableDeltaEncoding = false;
                m_enablePostingListRearrange = false;
                m_enableDataCompression = false;
                m_enableDictTraining = true;
            }

            virtual ~ExtraStaticSearcher()
            {
            }

            virtual bool LoadIndex(Options& p_opt, COMMON::VersionLabel& p_versionMap) {
                m_extraFullGraphFile = p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;
                std::string curFile = m_extraFullGraphFile;
                do {
                    auto curIndexFile = f_createAsyncIO();
                    if (curIndexFile == nullptr || !curIndexFile->Initialize(curFile.c_str(), std::ios::binary | std::ios::in, 
#ifndef _MSC_VER
#ifdef BATCH_READ
                        p_opt.m_searchInternalResultNum, 2, 2, p_opt.m_iSSDNumberOfThreads
#else
                        p_opt.m_searchInternalResultNum * p_opt.m_iSSDNumberOfThreads / p_opt.m_ioThreads + 1, 2, 2, p_opt.m_ioThreads
#endif
/*
#ifdef BATCH_READ
                        max(p_opt.m_searchInternalResultNum*m_vectorInfoSize, 1 << 12), 2, 2, p_opt.m_iSSDNumberOfThreads
#else
                        p_opt.m_searchInternalResultNum* p_opt.m_iSSDNumberOfThreads / p_opt.m_ioThreads + 1, 2, 2, p_opt.m_ioThreads
#endif
*/
#else
                        (p_opt.m_searchPostingPageLimit + 1) * PageSize, 2, 2, (std::uint16_t)p_opt.m_ioThreads
#endif
                    )) {
                        LOG(Helper::LogLevel::LL_Error, "Cannot open file:%s!\n", curFile.c_str());
                        return false;
                    }

                    m_indexFiles.emplace_back(curIndexFile);
                    try {
                        m_totalListCount += LoadingHeadInfo(curFile, p_opt.m_searchPostingPageLimit, m_listInfos);
                    } 
                    catch (std::exception& e)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Error occurs when loading HeadInfo:%s\n", e.what());
                        return false;
                    }

                    curFile = m_extraFullGraphFile + "_" + std::to_string(m_indexFiles.size());
                } while (fileexists(curFile.c_str()));
                m_oneContext = (m_indexFiles.size() == 1);

                m_enableDeltaEncoding = p_opt.m_enableDeltaEncoding;
                m_enablePostingListRearrange = p_opt.m_enablePostingListRearrange;
                m_enableDataCompression = p_opt.m_enableDataCompression;
                m_enableDictTraining = p_opt.m_enableDictTraining;

                if (m_enablePostingListRearrange) m_parsePosting = &ExtraStaticSearcher<ValueType>::ParsePostingListRearrange;
                else m_parsePosting = &ExtraStaticSearcher<ValueType>::ParsePostingList;
                if (m_enableDeltaEncoding) m_parseEncoding = &ExtraStaticSearcher<ValueType>::ParseDeltaEncoding;
                else m_parseEncoding = &ExtraStaticSearcher<ValueType>::ParseEncoding;
                
                m_listPerFile = static_cast<int>((m_totalListCount + m_indexFiles.size() - 1) / m_indexFiles.size());

#ifndef _MSC_VER
                Helper::AIOTimeout.tv_nsec = p_opt.m_iotimeout * 1000;
#endif
                return true;
            }

            virtual void SearchIndex(ExtraWorkSpace* p_exWorkSpace,
                QueryResult& p_queryResults,
                std::shared_ptr<VectorIndex> p_index,
                SearchStats* p_stats,
                std::set<int>* truth, std::map<int, std::set<int>>* found)
            {
                const uint32_t postingListCount = static_cast<uint32_t>(p_exWorkSpace->m_postingIDs.size());

                COMMON::QueryResultSet<ValueType>& queryResults = *((COMMON::QueryResultSet<ValueType>*)&p_queryResults);
 
                int diskRead = 0;
                int diskIO = 0;
                int listElements = 0;

#if defined(ASYNC_READ) && !defined(BATCH_READ)
                int unprocessed = 0;
#endif
#ifdef BATCH_READ
                uint32_t queuedReadCount = 0;
#endif

                for (uint32_t pi = 0; pi < postingListCount; ++pi)
                {
                    auto curPostingID = p_exWorkSpace->m_postingIDs[pi];
                    ListInfo* listInfo = &(m_listInfos[curPostingID]);
                    int fileid = m_oneContext? 0: curPostingID / m_listPerFile;

#ifndef BATCH_READ
                    Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

                    listElements += listInfo->listEleCount;

                    size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
                    char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

                    std::vector<PageReadRange> partialRanges;
                    std::uint64_t loadedPageMask = 0;
                    if (truth == nullptr
                        && BuildPartialReadRanges(curPostingID, p_exWorkSpace, partialRanges, loadedPageMask))
                    {
                        Helper::DiskIO* partialIndexFile = m_indexFiles[fileid].get();
                        for (const auto& range : partialRanges)
                        {
                            const std::uint64_t rangeOffset =
                                listInfo->listOffset + (static_cast<std::uint64_t>(range.firstPage) << PageSizeEx);
                            const std::uint64_t rangeBytes =
                                static_cast<std::uint64_t>(range.pageCount) << PageSizeEx;
                            char* rangeBuffer =
                                buffer + (static_cast<std::uint64_t>(range.firstPage) << PageSizeEx);
                            auto numRead = partialIndexFile->ReadBinary(rangeBytes, rangeBuffer, rangeOffset);
                            if (numRead != rangeBytes) {
                                LOG(Helper::LogLevel::LL_Error, "File %s partial read bytes, expected: %zu, actual: %llu.\n", m_extraFullGraphFile.c_str(), static_cast<size_t>(rangeBytes), numRead);
                                throw std::runtime_error("File partial read mismatch");
                            }
                        }
                        diskRead += CountBits(loadedPageMask);
                        diskIO += static_cast<int>(partialRanges.size());
                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        ProcessPostingPartial(loadedPageMask);
                        continue;
                    }

                    diskRead += listInfo->listPageCount;
                    diskIO += 1;

#ifdef ASYNC_READ       
#ifdef BATCH_READ
                    auto& request = p_exWorkSpace->m_diskRequests[queuedReadCount++];
#else
                    auto& request = p_exWorkSpace->m_diskRequests[pi];
#endif
                    request.m_offset = listInfo->listOffset;
                    request.m_readSize = totalBytes;
                    request.m_buffer = buffer;
                    request.m_status = (fileid << 16) | p_exWorkSpace->m_spaceID;
                    request.m_payload = (void*)listInfo; 
                    request.m_success = false;

#ifdef BATCH_READ // async batch read
                    request.m_callback = [&p_exWorkSpace, &queryResults, &p_index, &request, this](bool success)
                    {
                        char* buffer = request.m_buffer;
                        ListInfo* listInfo = (ListInfo*)(request.m_payload);

                        // decompress posting list
                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            DecompressPosting();
                        }

                        ProcessPosting();
                    };
#else // async read
                    request.m_callback = [&p_exWorkSpace, &request](bool success)
                    {
                        p_exWorkSpace->m_processIocp.push(&request);
                    };

                    ++unprocessed;
                    if (!(indexFile->ReadFileAsync(request)))
                    {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read file!\n");
                        unprocessed--;
                    }
#endif
#else // sync read
                    auto numRead = indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
                    if (numRead != totalBytes) {
                        LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                        throw std::runtime_error("File read mismatch");
                    }
                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
#endif
                }

#ifdef ASYNC_READ
#ifdef BATCH_READ
                if (queuedReadCount > 0) {
                    BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(), queuedReadCount);
                }
#else
                while (unprocessed > 0)
                {
                    Helper::AsyncReadRequest* request;
                    if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

                    --unprocessed;
                    char* buffer = request->m_buffer;
                    ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
                }
#endif
#endif
                if (truth) {
                    for (uint32_t pi = 0; pi < postingListCount; ++pi)
                    {
                        auto curPostingID = p_exWorkSpace->m_postingIDs[pi];

                        ListInfo* listInfo = &(m_listInfos[curPostingID]);
                        char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            p_postingListFullData = (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer();
                            if (listInfo->listEleCount != 0)
                            {
                                try {
                                    m_pCompressor->Decompress(buffer + listInfo->pageOffset, listInfo->listTotalBytes, p_postingListFullData, listInfo->listEleCount * m_vectorInfoSize, m_enableDictTraining);
                                }
                                catch (std::runtime_error& err) {
                                    LOG(Helper::LogLevel::LL_Error, "Decompress postingList %d  failed! %s, \n", curPostingID, err.what());
                                    continue;
                                }
                            }
                        }

                        for (size_t i = 0; i < listInfo->listEleCount; ++i) {
                            uint64_t offsetVectorID = m_enablePostingListRearrange ? (m_vectorInfoSize - sizeof(int)) * listInfo->listEleCount + sizeof(int) * i : m_vectorInfoSize * i; \
                            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID)); \
                            if (truth && truth->count(vectorID)) (*found)[curPostingID].insert(vectorID);
                        }
                    }
                }

                if (p_stats) 
                {
                    p_stats->m_totalListElementsCount = listElements;
                    p_stats->m_diskIOCount = diskIO;
                    p_stats->m_diskAccessCount = diskRead;
                }
            }

            std::string GetPostingListFullData(
                int postingListId,
                size_t p_postingListSize,
                Selection &p_selections,
                std::shared_ptr<VectorSet> p_fullVectors,
                bool m_enableDeltaEncoding = false,
                bool m_enablePostingListRearrange = false,
                const ValueType *headVector = nullptr)
            {
                std::string postingListFullData("");
                std::string vectors("");
                std::string vectorIDs("");
                size_t selectIdx = p_selections.lower_bound(postingListId);
                // iterate over all the vectors in the posting list
                for (int i = 0; i < p_postingListSize; ++i)
                {
                    if (p_selections[selectIdx].node != postingListId)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Selection ID NOT MATCH! node:%d offset:%zu\n", postingListId, selectIdx);
                        throw std::runtime_error("Selection ID mismatch");
                    }
                    std::string vectorID("");
                    std::string vector("");

                    int vid = p_selections[selectIdx++].tonode;
                    vectorID.append(reinterpret_cast<char *>(&vid), sizeof(int));

                    ValueType *p_vector = reinterpret_cast<ValueType *>(p_fullVectors->GetVector(vid));
                    if (m_enableDeltaEncoding)
                    {
                        DimensionType n = p_fullVectors->Dimension();
                        std::vector<ValueType> p_vector_delta(n);
                        for (auto j = 0; j < n; j++)
                        {
                            p_vector_delta[j] = p_vector[j] - headVector[j];
                        }
                        vector.append(reinterpret_cast<char *>(&p_vector_delta[0]), p_fullVectors->PerVectorDataSize());
                    }
                    else
                    {
                        vector.append(reinterpret_cast<char *>(p_vector), p_fullVectors->PerVectorDataSize());
                    }

                    if (m_enablePostingListRearrange)
                    {
                        vectorIDs += vectorID;
                        vectors += vector;
                    }
                    else
                    {
                        postingListFullData += (vectorID + vector);
                    }
                }
                if (m_enablePostingListRearrange)
                {
                    return vectors + vectorIDs;
                }
                return postingListFullData;
            }

            bool BuildIndex(std::shared_ptr<Helper::VectorSetReader>& p_reader, std::shared_ptr<VectorIndex> p_headIndex, Options& p_opt, COMMON::VersionLabel& p_versionMap, SizeType upperBound = -1) {
                std::string outputFile = p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;
                if (outputFile.empty())
                {
                    LOG(Helper::LogLevel::LL_Error, "Output file can't be empty!\n");
                    return false;
                }

                int numThreads = p_opt.m_iSSDNumberOfThreads;
                int candidateNum = p_opt.m_internalResultNum;
                std::unordered_set<SizeType> headVectorIDS;
                if (p_opt.m_headIDFile.empty()) {
                    LOG(Helper::LogLevel::LL_Error, "Not found VectorIDTranslate!\n");
                    return false;
                }

                if (fileexists((p_opt.m_indexDirectory + FolderSep + p_opt.m_headIDFile).c_str()))
                {
                    auto ptr = SPTAG::f_createIO();
                    if (ptr == nullptr || !ptr->Initialize((p_opt.m_indexDirectory + FolderSep +  p_opt.m_headIDFile).c_str(), std::ios::binary | std::ios::in)) {
                        LOG(Helper::LogLevel::LL_Error, "failed open VectorIDTranslate: %s\n", p_opt.m_headIDFile.c_str());
                        return false;
                    }

                    std::uint64_t vid;
                    while (ptr->ReadBinary(sizeof(vid), reinterpret_cast<char*>(&vid)) == sizeof(vid))
                    {
                        headVectorIDS.insert(static_cast<SizeType>(vid));
                    }
                    LOG(Helper::LogLevel::LL_Info, "Loaded %u Vector IDs\n", static_cast<uint32_t>(headVectorIDS.size()));
                }

                SizeType fullCount = 0;
                size_t vectorInfoSize = 0;
                {
                    auto probeVectors = upperBound > 0
                        ? p_reader->GetVectorSet(0, std::min<SizeType>(upperBound, 1))
                        : p_reader->GetVectorSet();
                    fullCount = upperBound > 0 ? upperBound : probeVectors->Count();
                    vectorInfoSize = probeVectors->PerVectorDataSize() + sizeof(int);
                }

                Selection selections(static_cast<size_t>(fullCount) * p_opt.m_replicaCount, p_opt.m_tmpdir);
                LOG(Helper::LogLevel::LL_Info, "Full vector count:%d Edge bytes:%llu selection size:%zu, capacity size:%zu\n", fullCount, sizeof(Edge), selections.m_selections.size(), selections.m_selections.capacity());
                std::vector<std::atomic_int> replicaCount(fullCount);
                std::vector<std::atomic_int> postingListSize(p_headIndex->GetNumSamples());
                for (auto& pls : postingListSize) pls = 0;
                std::unordered_set<SizeType> emptySet;
                SizeType batchSize = (fullCount + p_opt.m_batches - 1) / p_opt.m_batches;

                auto t1 = std::chrono::high_resolution_clock::now();
                if (p_opt.m_batches > 1)
                {
                    if (selections.SaveBatch() != ErrorCode::Success)
                    {
                        return false;
                    }
                }
                {
                    LOG(Helper::LogLevel::LL_Info, "Preparation done, start candidate searching.\n");
                    SizeType sampleSize = p_opt.m_samples;
                    std::vector<SizeType> samples(sampleSize, 0);
                    for (int i = 0; i < p_opt.m_batches; i++) {
                        SizeType start = i * batchSize;
                        SizeType end = min(start + batchSize, fullCount);
                        auto fullVectors = p_reader->GetVectorSet(start, end);
                        if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine && !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer) fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

                        if (p_opt.m_batches > 1) {
                            if (selections.LoadBatch(static_cast<size_t>(start) * p_opt.m_replicaCount, static_cast<size_t>(end) * p_opt.m_replicaCount) != ErrorCode::Success)
                            {
                                return false;
                            }
                            emptySet.clear();
                            for (auto vid : headVectorIDS) {
                                if (vid >= start && vid < end) emptySet.insert(vid - start);
                            }
                        }
                        else {
                            emptySet = headVectorIDS;
                        }

                        int sampleNum = 0;
                        for (int j = start; j < end && sampleNum < sampleSize; j++)
                        {
                            if (headVectorIDS.count(j) == 0) samples[sampleNum++] = j - start;
                        }

                        float acc = 0;
#pragma omp parallel for schedule(dynamic)
                        for (int j = 0; j < sampleNum; j++)
                        {
                            COMMON::Utils::atomic_float_add(&acc, COMMON::TruthSet::CalculateRecall(p_headIndex.get(), fullVectors->GetVector(samples[j]), candidateNum));
                        }
                        acc = acc / sampleNum;
                        LOG(Helper::LogLevel::LL_Info, "Batch %d vector(%d,%d) loaded with %d vectors (%zu) HeadIndex acc @%d:%f.\n", i, start, end, fullVectors->Count(), selections.m_selections.size(), candidateNum, acc);

                        p_headIndex->ApproximateRNG(fullVectors, emptySet, candidateNum, selections.m_selections.data(), p_opt.m_replicaCount, numThreads, p_opt.m_gpuSSDNumTrees, p_opt.m_gpuSSDLeafSize, p_opt.m_rngFactor, p_opt.m_numGPUs);
                        LOG(Helper::LogLevel::LL_Info, "Batch %d finished!\n", i);

                        for (SizeType j = start; j < end; j++) {
                            replicaCount[j] = 0;
                            size_t vecOffset = j * (size_t)p_opt.m_replicaCount;
                            if (headVectorIDS.count(j) == 0) {
                                for (int resNum = 0; resNum < p_opt.m_replicaCount && selections[vecOffset + resNum].node != INT_MAX; resNum++) {
                                    ++postingListSize[selections[vecOffset + resNum].node];
                                    selections[vecOffset + resNum].tonode = j;
                                    ++replicaCount[j];
                                }
                            }
                        }

                        if (p_opt.m_batches > 1)
                        {
                            if (selections.SaveBatch() != ErrorCode::Success)
                            {
                                return false;
                            }
                        }
                    }
                }
                auto t2 = std::chrono::high_resolution_clock::now();
                LOG(Helper::LogLevel::LL_Info, "Searching replicas ended. Search Time: %.2lf mins\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count()) / 60.0);

                if (p_opt.m_batches > 1)
                {
                    if (selections.LoadBatch(0, static_cast<size_t>(fullCount) * p_opt.m_replicaCount) != ErrorCode::Success)
                    {
                        return false;
                    }
                }

                // Sort results either in CPU or GPU
                VectorIndex::SortSelections(&selections.m_selections);

                auto t3 = std::chrono::high_resolution_clock::now();
                LOG(Helper::LogLevel::LL_Info, "Time to sort selections:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t3 - t2).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count()) / 1000);

                int postingSizeLimit = INT_MAX;
                if (p_opt.m_postingPageLimit > 0)
                {
                    postingSizeLimit = static_cast<int>(p_opt.m_postingPageLimit * PageSize / vectorInfoSize);
                }

                LOG(Helper::LogLevel::LL_Info, "Posting size limit: %d\n", postingSizeLimit);

                {
                    std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);
                    for (int i = 0; i < replicaCount.size(); ++i)
                    {
                        if (headVectorIDS.count(i) > 0) continue;
                        ++replicaCountDist[replicaCount[i]];
                    }

                    LOG(Helper::LogLevel::LL_Info, "Before Posting Cut:\n");
                    for (int i = 0; i < replicaCountDist.size(); ++i)
                    {
                        LOG(Helper::LogLevel::LL_Info, "Replica Count Dist: %d, %d\n", i, replicaCountDist[i]);
                    }
                }

#pragma omp parallel for schedule(dynamic)
                for (int i = 0; i < postingListSize.size(); ++i)
                {
                    if (postingListSize[i] <= postingSizeLimit) continue;

                    std::size_t selectIdx = std::lower_bound(selections.m_selections.begin(), selections.m_selections.end(), i, Selection::g_edgeComparer) - selections.m_selections.begin();

                    for (size_t dropID = postingSizeLimit; dropID < postingListSize[i]; ++dropID)
                    {
                        int tonode = selections.m_selections[selectIdx + dropID].tonode;
                        --replicaCount[tonode];
                    }
                    postingListSize[i] = postingSizeLimit;
                }

                if (p_opt.m_outputEmptyReplicaID)
                {
                    std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);
                    auto ptr = SPTAG::f_createIO();
                    if (ptr == nullptr || !ptr->Initialize("EmptyReplicaID.bin", std::ios::binary | std::ios::out)) {
                        LOG(Helper::LogLevel::LL_Error, "Fail to create EmptyReplicaID.bin!\n");
                        return false;
                    }
                    for (int i = 0; i < replicaCount.size(); ++i)
                    {
                        if (headVectorIDS.count(i) > 0) continue;

                        ++replicaCountDist[replicaCount[i]];

                        if (replicaCount[i] < 2)
                        {
                            long long vid = i;
                            if (ptr->WriteBinary(sizeof(vid), reinterpret_cast<char*>(&vid)) != sizeof(vid)) {
                                LOG(Helper::LogLevel::LL_Error, "Failt to write EmptyReplicaID.bin!");
                                return false;
                            }
                        }
                    }

                    LOG(Helper::LogLevel::LL_Info, "After Posting Cut:\n");
                    for (int i = 0; i < replicaCountDist.size(); ++i)
                    {
                        LOG(Helper::LogLevel::LL_Info, "Replica Count Dist: %d, %d\n", i, replicaCountDist[i]);
                    }
                }

                auto t4 = std::chrono::high_resolution_clock::now();
                LOG(SPTAG::Helper::LogLevel::LL_Info, "Time to perform posting cut:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t4 - t3).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count()) / 1000);

                // number of posting lists per file
                size_t postingFileSize = (postingListSize.size() + p_opt.m_ssdIndexFileNum - 1) / p_opt.m_ssdIndexFileNum;
                std::vector<size_t> selectionsBatchOffset(p_opt.m_ssdIndexFileNum + 1, 0);
                for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
                    size_t curPostingListEnd = min(postingListSize.size(), (i + 1) * postingFileSize);
                    selectionsBatchOffset[i + 1] = std::lower_bound(selections.m_selections.begin(), selections.m_selections.end(), (SizeType)curPostingListEnd, Selection::g_edgeComparer) - selections.m_selections.begin();
                }

                if (p_opt.m_ssdIndexFileNum > 1)
                {
                    if (selections.SaveBatch() != ErrorCode::Success)
                    {
                        return false;
                    }
                }

                auto fullVectors = p_reader->GetVectorSet();
                if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine && !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer) fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

                // iterate over files
                for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
                    size_t curPostingListOffSet = i * postingFileSize;
                    size_t curPostingListEnd = min(postingListSize.size(), (i + 1) * postingFileSize);
                    // postingListSize: number of vectors in the posting list, type vector<int>
                    std::vector<int> curPostingListSizes(
                        postingListSize.begin() + curPostingListOffSet,
                        postingListSize.begin() + curPostingListEnd);

                    std::vector<size_t> curPostingListBytes(curPostingListSizes.size());
                    
                    if (p_opt.m_ssdIndexFileNum > 1)
                    {
                        if (selections.LoadBatch(selectionsBatchOffset[i], selectionsBatchOffset[i + 1]) != ErrorCode::Success)
                        {
                            return false;
                        }
                    }
                    // create compressor
                    if (p_opt.m_enableDataCompression && i == 0)
                    {
                        m_pCompressor = std::make_unique<Compressor>(p_opt.m_zstdCompressLevel, p_opt.m_dictBufferCapacity);
                        // train dict
                        if (p_opt.m_enableDictTraining) {
                            LOG(Helper::LogLevel::LL_Info, "Training dictionary...\n");
                            std::string samplesBuffer("");
                            std::vector<size_t> samplesSizes;
                            for (int j = 0; j < curPostingListSizes.size(); j++) {
                                if (curPostingListSizes[j] == 0) {
                                    continue;
                                }
                                ValueType* headVector = nullptr;
                                if (p_opt.m_enableDeltaEncoding)
                                {
                                    headVector = (ValueType*)p_headIndex->GetSample(j);
                                }
                                std::string postingListFullData = GetPostingListFullData(
                                    j, curPostingListSizes[j], selections, fullVectors, p_opt.m_enableDeltaEncoding, p_opt.m_enablePostingListRearrange, headVector);

                                samplesBuffer += postingListFullData;
                                samplesSizes.push_back(postingListFullData.size());
                                if (samplesBuffer.size() > p_opt.m_minDictTraingBufferSize) break;
                            }
                            LOG(Helper::LogLevel::LL_Info, "Using the first %zu postingLists to train dictionary... \n", samplesSizes.size());
                            std::size_t dictSize = m_pCompressor->TrainDict(samplesBuffer, &samplesSizes[0], (unsigned int)samplesSizes.size());
                            LOG(Helper::LogLevel::LL_Info, "Dictionary trained, dictionary size: %zu \n", dictSize);
                        }
                    }

                    if (p_opt.m_enableDataCompression) {
                        LOG(Helper::LogLevel::LL_Info, "Getting compressed size of each posting list...\n");
#pragma omp parallel for schedule(dynamic)
                        for (int j = 0; j < curPostingListSizes.size(); j++) 
                        {
                            SizeType postingListId = j + (SizeType)curPostingListOffSet;
                            // do not compress if no data
                            if (postingListSize[postingListId] == 0) {
                                curPostingListBytes[j] = 0;
                                continue;
                            }
                            ValueType* headVector = nullptr;
                            if (p_opt.m_enableDeltaEncoding)
                            {
                                headVector = (ValueType*)p_headIndex->GetSample(postingListId);
                            }
                            std::string postingListFullData = GetPostingListFullData(
                                postingListId, postingListSize[postingListId], selections, fullVectors, p_opt.m_enableDeltaEncoding, p_opt.m_enablePostingListRearrange, headVector);
                            size_t sizeToCompress = postingListSize[postingListId] * vectorInfoSize;
                            if (sizeToCompress != postingListFullData.size()) {
                                LOG(Helper::LogLevel::LL_Error, "Size to compress NOT MATCH! PostingListFullData size: %zu sizeToCompress: %zu \n", postingListFullData.size(), sizeToCompress);
                            }
                            curPostingListBytes[j] = m_pCompressor->GetCompressedSize(postingListFullData, p_opt.m_enableDictTraining);
                            if (postingListId % 10000 == 0 || curPostingListBytes[j] > static_cast<uint64_t>(p_opt.m_postingPageLimit) * PageSize) {
                                LOG(Helper::LogLevel::LL_Info, "Posting list %d/%d, compressed size: %d, compression ratio: %.4f\n", postingListId, postingListSize.size(), curPostingListBytes[j], curPostingListBytes[j] / float(sizeToCompress));
                            }
                        }
                        LOG(Helper::LogLevel::LL_Info, "Getted compressed size for all the %d posting lists in SSD Index file %d.\n", curPostingListBytes.size(), i);
                        LOG(Helper::LogLevel::LL_Info, "Mean compressed size: %.4f \n", std::accumulate(curPostingListBytes.begin(), curPostingListBytes.end(), 0.0) / curPostingListBytes.size());
                        LOG(Helper::LogLevel::LL_Info, "Mean compression ratio: %.4f \n", std::accumulate(curPostingListBytes.begin(), curPostingListBytes.end(), 0.0) / (std::accumulate(curPostingListSizes.begin(), curPostingListSizes.end(), 0.0) * vectorInfoSize));
                    }
                    else {
                        for (int j = 0; j < curPostingListSizes.size(); j++)
                        {
                            curPostingListBytes[j] = curPostingListSizes[j] * vectorInfoSize;
                        }
                    }

                    std::unique_ptr<int[]> postPageNum;
                    std::unique_ptr<std::uint16_t[]> postPageOffset;
                    std::vector<int> postingOrderInIndex;
                    SelectPostingOffset(curPostingListBytes, postPageNum, postPageOffset, postingOrderInIndex);

                    OutputSSDIndexFile((i == 0) ? outputFile : outputFile + "_" + std::to_string(i),
                        p_opt.m_enableDeltaEncoding,
                        p_opt.m_enablePostingListRearrange,
                        p_opt.m_enableDataCompression,
                        p_opt.m_enableDictTraining,
                        vectorInfoSize,
                        curPostingListSizes,
                        curPostingListBytes,
                        p_headIndex,
                        selections,
                        postPageNum,
                        postPageOffset,
                        postingOrderInIndex,
                        fullVectors,
                        curPostingListOffSet);
                }

                auto t5 = std::chrono::high_resolution_clock::now();
                auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(t5 - t1).count();
                LOG(Helper::LogLevel::LL_Info, "Total used time: %.2lf minutes (about %.2lf hours).\n", elapsedSeconds / 60.0, elapsedSeconds / 3600.0);
             
                return true;
            }

            virtual bool CheckValidPosting(SizeType postingID)
            {
                return postingID >= 0
                    && static_cast<size_t>(postingID) < m_listInfos.size()
                    && m_listInfos[postingID].listEleCount != 0;
            }

            bool CheckPostingFilter(SizeType postingID, const ExtraWorkSpace* p_exWorkSpace) const override
            {
                if (!m_postingMaskSidecarLoaded
                    || p_exWorkSpace == nullptr
                    || !p_exWorkSpace->HasVectorFilter()
                    || postingID < 0
                    || static_cast<size_t>(postingID + 1) >= m_postingMaskOffsets.size())
                {
                    return true;
                }

                const auto begin = m_postingMaskOffsets[postingID];
                const auto end = m_postingMaskOffsets[postingID + 1];
                for (std::uint64_t idx = begin; idx < end; ++idx)
                {
                    if (p_exWorkSpace->CheckVectorFilter(static_cast<SizeType>(m_postingMaskVectorIDs[idx]))) {
                        return true;
                    }
                }
                return false;
            }

            bool BuildPostingMaskSidecar(const std::string& p_indexDirectory) override
            {
                (void)p_indexDirectory;
                if (!CanUsePostingMaskSidecar()) {
                    m_postingMaskSidecarLoaded = false;
                    return true;
                }
                if (m_listInfos.empty()) return true;

                const std::string path = PostingMaskSidecarPath();
                const auto startTime = std::chrono::steady_clock::now();
                const size_t totalLists = m_listInfos.size();
                const size_t progressStep = std::max<size_t>(1, (totalLists + 19) / 20);
                size_t nextProgress = progressStep;
                std::uint64_t pagesRead = 0;
                std::uint64_t bytesRead = 0;
                std::vector<std::uint64_t> offsets;
                std::vector<std::uint32_t> vectorIDs;
                std::vector<std::uint16_t> pageMasks;
                auto logProgress = [&](size_t completedLists) {
                    const double percent = totalLists == 0
                        ? 100.0
                        : (100.0 * static_cast<double>(completedLists) / static_cast<double>(totalLists));
                    const size_t filled = std::min<size_t>(20, static_cast<size_t>(percent / 5.0));
                    std::string bar(filled, '#');
                    bar.resize(20, '.');
                    const auto elapsedMs = static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - startTime)
                            .count());
                    LOG(Helper::LogLevel::LL_Info,
                        "Building posting mask sidecar [%s] %.1f%% lists=%zu/%zu entries=%zu pages_read=%llu bytes_read=%llu elapsed_ms=%llu path=%s\n",
                        bar.c_str(),
                        percent,
                        completedLists,
                        totalLists,
                        vectorIDs.size(),
                        static_cast<unsigned long long>(pagesRead),
                        static_cast<unsigned long long>(bytesRead),
                        static_cast<unsigned long long>(elapsedMs),
                        path.c_str());
                };

                offsets.reserve(m_listInfos.size() + 1);
                offsets.push_back(0);

                LOG(Helper::LogLevel::LL_Info, "Building posting mask sidecar start lists=%zu path=%s\n", totalLists, path.c_str());
                std::vector<char> buffer;
                for (size_t postingID = 0; postingID < m_listInfos.size(); ++postingID)
                {
                    const ListInfo* listInfo = &(m_listInfos[postingID]);
                    const size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
                    if (listInfo->listEleCount > 0 && totalBytes > 0)
                    {
                        buffer.resize(totalBytes);
                        const int fileid = m_oneContext ? 0 : static_cast<int>(postingID) / m_listPerFile;
                        Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
                        auto numRead = indexFile->ReadBinary(totalBytes, buffer.data(), listInfo->listOffset);
                        if (numRead != totalBytes) {
                            LOG(Helper::LogLevel::LL_Error, "Failed to build posting mask sidecar: file %s read bytes, expected: %zu, actual: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                            return false;
                        }
                        pagesRead += static_cast<std::uint64_t>(listInfo->listPageCount);
                        bytesRead += static_cast<std::uint64_t>(totalBytes);

                        char* p_postingListFullData = buffer.data() + listInfo->pageOffset;
                        for (int i = 0; i < listInfo->listEleCount; ++i)
                        {
                            std::uint64_t offsetVectorID = 0;
                            std::uint64_t offsetVector = 0;
                            (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount);
                            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));
                            if (vectorID < 0) continue;

                            vectorIDs.push_back(static_cast<std::uint32_t>(vectorID));
                            pageMasks.push_back(PostingEntryPageMask(listInfo, i, offsetVectorID, offsetVector));
                        }
                    }
                    offsets.push_back(static_cast<std::uint64_t>(vectorIDs.size()));
                    const size_t completedLists = postingID + 1;
                    if (completedLists >= nextProgress || completedLists == totalLists) {
                        logProgress(completedLists);
                        while (nextProgress <= completedLists) {
                            nextProgress += progressStep;
                        }
                    }
                }

                const std::string tmpPath = path + ".tmp";
                const auto scanElapsedMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - startTime)
                        .count());
                LOG(Helper::LogLevel::LL_Info,
                    "Building posting mask sidecar scan complete lists=%zu entries=%zu pages_read=%llu bytes_read=%llu elapsed_ms=%llu path=%s\n",
                    totalLists,
                    vectorIDs.size(),
                    static_cast<unsigned long long>(pagesRead),
                    static_cast<unsigned long long>(bytesRead),
                    static_cast<unsigned long long>(scanElapsedMs),
                    path.c_str());
                std::ofstream out(tmpPath, std::ios::binary | std::ios::trunc);
                if (!out.is_open()) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to open posting mask sidecar for write: %s\n", tmpPath.c_str());
                    return false;
                }

                const std::uint64_t magic = kPostingMaskSidecarMagic;
                const std::uint32_t version = kPostingMaskSidecarVersion;
                const std::uint32_t flags = kPostingMaskSidecarFlagPageMasks;
                const std::uint64_t listCount = static_cast<std::uint64_t>(m_listInfos.size());
                const std::uint64_t entryCount = static_cast<std::uint64_t>(vectorIDs.size());
                const std::uint64_t sourceBytes = PostingMaskSourceBytes();
                const std::uint64_t sourceTimestamp = PostingMaskSourceTimestamp();
                WriteScalar(out, magic);
                WriteScalar(out, version);
                WriteScalar(out, flags);
                WriteScalar(out, listCount);
                WriteScalar(out, entryCount);
                WriteScalar(out, sourceBytes);
                WriteScalar(out, sourceTimestamp);
                WriteVector(out, offsets);
                WriteVector(out, vectorIDs);
                WriteVector(out, pageMasks);
                out.close();
                if (!out.good()) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write posting mask sidecar: %s\n", tmpPath.c_str());
                    std::remove(tmpPath.c_str());
                    return false;
                }
                std::remove(path.c_str());
                if (std::rename(tmpPath.c_str(), path.c_str()) != 0) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to rename posting mask sidecar %s to %s\n", tmpPath.c_str(), path.c_str());
                    std::remove(tmpPath.c_str());
                    return false;
                }

                const auto elapsedMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - startTime)
                        .count());
                LOG(Helper::LogLevel::LL_Info,
                    "Built posting mask sidecar lists=%zu entries=%zu pages_read=%llu bytes_read=%llu elapsed_ms=%llu path=%s\n",
                    m_listInfos.size(),
                    vectorIDs.size(),
                    static_cast<unsigned long long>(pagesRead),
                    static_cast<unsigned long long>(bytesRead),
                    static_cast<unsigned long long>(elapsedMs),
                    path.c_str());
                return true;
            }

            bool LoadPostingMaskSidecar(const std::string& p_indexDirectory) override
            {
                (void)p_indexDirectory;
                m_postingMaskSidecarLoaded = false;
                m_postingMaskOffsets.clear();
                m_postingMaskVectorIDs.clear();
                m_postingMaskPageMasks.clear();

                if (!CanUsePostingMaskSidecar()) return true;

                const std::string path = PostingMaskSidecarPath();
                std::ifstream in(path, std::ios::binary);
                if (!in.is_open()) return false;

                std::uint64_t magic = 0;
                std::uint32_t version = 0;
                std::uint32_t flags = 0;
                std::uint64_t listCount = 0;
                std::uint64_t entryCount = 0;
                std::uint64_t sourceBytes = 0;
                std::uint64_t sourceTimestamp = 0;
                if (!ReadScalar(in, magic)
                    || !ReadScalar(in, version)
                    || !ReadScalar(in, flags)
                    || !ReadScalar(in, listCount)
                    || !ReadScalar(in, entryCount)
                    || !ReadScalar(in, sourceBytes)
                    || !ReadScalar(in, sourceTimestamp)
                    || magic != kPostingMaskSidecarMagic
                    || version != kPostingMaskSidecarVersion
                    || listCount != static_cast<std::uint64_t>(m_listInfos.size())
                    || sourceBytes != PostingMaskSourceBytes()
                    || sourceTimestamp != PostingMaskSourceTimestamp())
                {
                    return false;
                }

                m_postingMaskOffsets.resize(static_cast<size_t>(listCount) + 1);
                m_postingMaskVectorIDs.resize(static_cast<size_t>(entryCount));
                m_postingMaskPageMasks.resize(static_cast<size_t>(entryCount));
                if (!ReadVector(in, m_postingMaskOffsets)
                    || !ReadVector(in, m_postingMaskVectorIDs)
                    || !ReadVector(in, m_postingMaskPageMasks)
                    || m_postingMaskOffsets.empty()
                    || m_postingMaskOffsets.back() != entryCount)
                {
                    m_postingMaskOffsets.clear();
                    m_postingMaskVectorIDs.clear();
                    m_postingMaskPageMasks.clear();
                    return false;
                }
                for (size_t i = 1; i < m_postingMaskOffsets.size(); ++i)
                {
                    if (m_postingMaskOffsets[i] < m_postingMaskOffsets[i - 1]
                        || m_postingMaskOffsets[i] > entryCount)
                    {
                        m_postingMaskOffsets.clear();
                        m_postingMaskVectorIDs.clear();
                        m_postingMaskPageMasks.clear();
                        return false;
                    }
                }

                m_postingMaskSidecarLoaded = true;
                m_postingMaskSidecarHasPageMasks =
                    (flags & kPostingMaskSidecarFlagPageMasks) != 0
                    && m_postingMaskPageMasks.size() == m_postingMaskVectorIDs.size()
                    && CanUsePartialPostingPageReads();
                LOG(Helper::LogLevel::LL_Info, "Loaded posting mask sidecar lists=%llu entries=%llu partial_pages=%s path=%s\n",
                    static_cast<unsigned long long>(listCount),
                    static_cast<unsigned long long>(entryCount),
                    m_postingMaskSidecarHasPageMasks ? "true" : "false",
                    path.c_str());
                return true;
            }

        private:
            static constexpr std::uint64_t kPostingMaskSidecarMagic = 0x3153434d4c515350ULL; // PSQLMCS1, little endian
            static constexpr std::uint32_t kPostingMaskSidecarVersion = 2;
            static constexpr std::uint32_t kPostingMaskSidecarFlagPageMasks = 1;

            bool CanUsePostingMaskSidecar() const
            {
                return !m_enableDataCompression && m_vectorInfoSize > 0;
            }

            bool CanUsePartialPostingPageReads() const
            {
                return CanUsePostingMaskSidecar()
                    && !m_enablePostingListRearrange
                    && !m_enableDeltaEncoding;
            }

            std::string PostingMaskSidecarPath() const
            {
                return m_extraFullGraphFile + ".tpsql_mask_sidecar";
            }

            std::uint64_t PostingMaskSourceBytes() const
            {
                return TPSQL::FileSizeBytes(m_extraFullGraphFile);
            }

            std::uint64_t PostingMaskSourceTimestamp() const
            {
                std::error_code error;
                const auto timestamp = std::filesystem::last_write_time(m_extraFullGraphFile, error);
                if (error) return 0;
                return static_cast<std::uint64_t>(timestamp.time_since_epoch().count());
            }

            template <typename TScalar>
            static void WriteScalar(std::ofstream& out, TScalar value)
            {
                out.write(reinterpret_cast<const char*>(&value), sizeof(value));
            }

            template <typename TVector>
            static void WriteVector(std::ofstream& out, const std::vector<TVector>& values)
            {
                if (values.empty()) return;
                out.write(
                    reinterpret_cast<const char*>(values.data()),
                    static_cast<std::streamsize>(sizeof(TVector) * values.size()));
            }

            template <typename TScalar>
            static bool ReadScalar(std::ifstream& in, TScalar& value)
            {
                in.read(reinterpret_cast<char*>(&value), sizeof(value));
                return in.good();
            }

            template <typename TVector>
            static bool ReadVector(std::ifstream& in, std::vector<TVector>& values)
            {
                if (values.empty()) return true;
                in.read(
                    reinterpret_cast<char*>(values.data()),
                    static_cast<std::streamsize>(sizeof(TVector) * values.size()));
                return in.good();
            }

            static int CountBits(std::uint64_t value)
            {
                int count = 0;
                while (value != 0)
                {
                    value &= value - 1;
                    ++count;
                }
                return count;
            }

            std::uint64_t PageMaskForByteRange(std::uint64_t startByte, std::uint64_t endByte) const
            {
                const std::uint64_t firstPage = startByte >> PageSizeEx;
                const std::uint64_t lastPage = endByte >> PageSizeEx;
                if (lastPage >= 64) return UINT64_MAX;

                std::uint64_t mask = 0;
                for (std::uint64_t page = firstPage; page <= lastPage; ++page)
                {
                    mask |= (std::uint64_t(1) << page);
                }
                return mask;
            }

            std::uint16_t PostingEntryPageMask(
                const ListInfo* listInfo,
                int entryIndex,
                std::uint64_t offsetVectorID,
                std::uint64_t offsetVector) const
            {
                if (!CanUsePartialPostingPageReads()) return 0;

                const std::uint64_t recordStart =
                    static_cast<std::uint64_t>(listInfo->pageOffset)
                    + static_cast<std::uint64_t>(entryIndex) * static_cast<std::uint64_t>(m_vectorInfoSize);
                const std::uint64_t recordEnd =
                    recordStart + static_cast<std::uint64_t>(m_vectorInfoSize) - 1;
                const std::uint64_t vectorIdStart =
                    static_cast<std::uint64_t>(listInfo->pageOffset) + offsetVectorID;
                const std::uint64_t vectorEnd =
                    static_cast<std::uint64_t>(listInfo->pageOffset)
                    + offsetVector
                    + static_cast<std::uint64_t>(m_iDataDimension) * sizeof(ValueType)
                    - 1;
                const std::uint64_t start = std::min(recordStart, vectorIdStart);
                const std::uint64_t end = std::max(recordEnd, vectorEnd);
                const std::uint64_t mask = PageMaskForByteRange(start, end);
                if ((mask & ~std::uint64_t(UINT16_MAX)) != 0) return UINT16_MAX;
                return static_cast<std::uint16_t>(mask);
            }

            bool BuildPartialReadRanges(
                SizeType postingID,
                const ExtraWorkSpace* p_exWorkSpace,
                std::vector<PageReadRange>& ranges,
                std::uint64_t& loadedPageMask) const
            {
                ranges.clear();
                loadedPageMask = 0;
                if (!m_postingMaskSidecarLoaded
                    || !m_postingMaskSidecarHasPageMasks
                    || p_exWorkSpace == nullptr
                    || !p_exWorkSpace->HasVectorFilter()
                    || postingID < 0
                    || static_cast<size_t>(postingID + 1) >= m_postingMaskOffsets.size()
                    || static_cast<size_t>(postingID) >= m_listInfos.size())
                {
                    return false;
                }

                const ListInfo* listInfo = &(m_listInfos[postingID]);
                if (listInfo->listPageCount == 0 || listInfo->listPageCount > 16) return false;

                const std::uint64_t validPageMask =
                    (std::uint64_t(1) << listInfo->listPageCount) - 1;
                const auto begin = m_postingMaskOffsets[postingID];
                const auto end = m_postingMaskOffsets[postingID + 1];
                for (std::uint64_t idx = begin; idx < end; ++idx)
                {
                    if (p_exWorkSpace->CheckVectorFilter(static_cast<SizeType>(m_postingMaskVectorIDs[idx]))) {
                        loadedPageMask |= m_postingMaskPageMasks[idx];
                    }
                }
                loadedPageMask &= validPageMask;
                if (loadedPageMask == 0) return true;

                int matchingPages = 0;
                for (std::uint16_t page = 0; page < listInfo->listPageCount; ++page)
                {
                    if ((loadedPageMask & (std::uint64_t(1) << page)) == 0) continue;
                    ++matchingPages;
                    const std::uint16_t firstPage = page;
                    std::uint16_t pageCount = 1;
                    while (page + 1 < listInfo->listPageCount
                        && (loadedPageMask & (std::uint64_t(1) << (page + 1))) != 0)
                    {
                        ++page;
                        ++pageCount;
                        ++matchingPages;
                    }
                    ranges.push_back(PageReadRange{firstPage, pageCount});
                }

                if (ranges.size() == 1 && matchingPages <= 3) return true;
                if (ranges.size() <= 2 && matchingPages <= 2) return true;

                ranges.clear();
                loadedPageMask = 0;
                return false;
            }

            int LoadingHeadInfo(const std::string& p_file, int p_postingPageLimit, std::vector<ListInfo>& m_listInfos)
            {
                const auto loadStart = std::chrono::steady_clock::now();
                const std::uint64_t fileBytes = TPSQL::FileSizeBytes(p_file);
                TPSQL::LogMemoryEvent(
                    "sptag_spann",
                    "ssd_header_load_start",
                    "ExtraStaticSearcher::LoadingHeadInfo",
                    p_file,
                    fileBytes);
                auto ptr = SPTAG::f_createIO();
                if (ptr == nullptr || !ptr->Initialize(p_file.c_str(), std::ios::binary | std::ios::in)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to open file: %s\n", p_file.c_str());
                    throw std::runtime_error("Failed open file in LoadingHeadInfo");
                }
                m_pCompressor = std::make_unique<Compressor>(); // no need compress level to decompress

                int m_listCount;
                int m_totalDocumentCount;
                int m_listPageOffset;

                if (ptr->ReadBinary(sizeof(m_listCount), reinterpret_cast<char*>(&m_listCount)) != sizeof(m_listCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_totalDocumentCount), reinterpret_cast<char*>(&m_totalDocumentCount)) != sizeof(m_totalDocumentCount)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_iDataDimension), reinterpret_cast<char*>(&m_iDataDimension)) != sizeof(m_iDataDimension)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_listPageOffset), reinterpret_cast<char*>(&m_listPageOffset)) != sizeof(m_listPageOffset)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }

                if (m_vectorInfoSize == 0) m_vectorInfoSize = m_iDataDimension * sizeof(ValueType) + sizeof(int);
                else if (m_vectorInfoSize != m_iDataDimension * sizeof(ValueType) + sizeof(int)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read head info file! DataDimension and ValueType are not match!\n");
                    throw std::runtime_error("DataDimension and ValueType don't match in LoadingHeadInfo");
                }

                size_t totalListCount = m_listInfos.size();
                m_listInfos.resize(totalListCount + m_listCount);

                size_t totalListElementCount = 0;

                std::map<int, int> pageCountDist;

                size_t biglistCount = 0;
                size_t biglistElementCount = 0;
                int pageNum;
                for (int i = 0; i < m_listCount; ++i)
                {
                    ListInfo* listInfo = &(m_listInfos[totalListCount + i]);

                    if (m_enableDataCompression)
                    {
                        if (ptr->ReadBinary(sizeof(listInfo->listTotalBytes), reinterpret_cast<char*>(&(listInfo->listTotalBytes))) != sizeof(listInfo->listTotalBytes)) {
                            LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                            throw std::runtime_error("Failed read file in LoadingHeadInfo");
                        }
                    }
                    if (ptr->ReadBinary(sizeof(pageNum), reinterpret_cast<char*>(&(pageNum))) != sizeof(pageNum)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->pageOffset), reinterpret_cast<char*>(&(listInfo->pageOffset))) != sizeof(listInfo->pageOffset)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->listEleCount), reinterpret_cast<char*>(&(listInfo->listEleCount))) != sizeof(listInfo->listEleCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->listPageCount), reinterpret_cast<char*>(&(listInfo->listPageCount))) != sizeof(listInfo->listPageCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    listInfo->listOffset = (static_cast<uint64_t>(m_listPageOffset + pageNum) << PageSizeEx);
                    if (!m_enableDataCompression)
                    {
                        listInfo->listTotalBytes = listInfo->listEleCount * m_vectorInfoSize;
                        listInfo->listEleCount = min(listInfo->listEleCount, (min(static_cast<int>(listInfo->listPageCount), p_postingPageLimit) << PageSizeEx) / m_vectorInfoSize);
                        listInfo->listPageCount = static_cast<std::uint16_t>(ceil((m_vectorInfoSize * listInfo->listEleCount + listInfo->pageOffset) * 1.0 / (1 << PageSizeEx)));
                    }
                    totalListElementCount += listInfo->listEleCount;
                    int pageCount = listInfo->listPageCount;

                    if (pageCount > 1)
                    {
                        ++biglistCount;
                        biglistElementCount += listInfo->listEleCount;
                    }

                    if (pageCountDist.count(pageCount) == 0)
                    {
                        pageCountDist[pageCount] = 1;
                    }
                    else
                    {
                        pageCountDist[pageCount] += 1;
                    }
                }

                if (m_enableDataCompression && m_enableDictTraining)
                {
                    size_t dictBufferSize;
                    if (ptr->ReadBinary(sizeof(size_t), reinterpret_cast<char*>(&dictBufferSize)) != sizeof(dictBufferSize)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    char* dictBuffer = new char[dictBufferSize];
                    if (ptr->ReadBinary(dictBufferSize, dictBuffer) != dictBufferSize) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    try {
                        m_pCompressor->SetDictBuffer(std::string(dictBuffer, dictBufferSize));
                    }
                    catch (std::runtime_error& err) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read head info file: %s \n", err.what());
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    delete[] dictBuffer;
                }

                LOG(Helper::LogLevel::LL_Info,
                    "Finish reading header info, list count %d, total doc count %d, dimension %d, list page offset %d.\n",
                    m_listCount,
                    m_totalDocumentCount,
                    m_iDataDimension,
                    m_listPageOffset);

                LOG(Helper::LogLevel::LL_Info,
                    "Big page (>4K): list count %zu, total element count %zu.\n",
                    biglistCount,
                    biglistElementCount);

                LOG(Helper::LogLevel::LL_Info, "Total Element Count: %llu\n", totalListElementCount);
                const auto loadElapsedMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - loadStart)
                        .count());
                TPSQL::LogMemoryEvent(
                    "sptag_spann",
                    "ssd_header_load_finish",
                    "ExtraStaticSearcher::LoadingHeadInfo",
                    p_file,
                    fileBytes,
                    static_cast<std::uint64_t>(m_listCount) * sizeof(ListInfo),
                    loadElapsedMs,
                    "list_count=" + std::to_string(m_listCount)
                        + " total_document_count=" + std::to_string(m_totalDocumentCount)
                        + " total_list_element_count=" + std::to_string(totalListElementCount)
                        + " biglist_count=" + std::to_string(biglistCount));

                for (auto& ele : pageCountDist)
                {
                    LOG(Helper::LogLevel::LL_Info, "Page Count Dist: %d %d\n", ele.first, ele.second);
                }

                return m_listCount;
            }

            inline void ParsePostingListRearrange(uint64_t& offsetVectorID, uint64_t& offsetVector, int i, int eleCount)
            {
                offsetVectorID = (m_vectorInfoSize - sizeof(int)) * eleCount + sizeof(int) * i;
                offsetVector = (m_vectorInfoSize - sizeof(int)) * i;
            }

            inline void ParsePostingList(uint64_t& offsetVectorID, uint64_t& offsetVector, int i, int eleCount)
            {
                offsetVectorID = m_vectorInfoSize * i;
                offsetVector = offsetVectorID + sizeof(int);
            }

            inline void ParseDeltaEncoding(std::shared_ptr<VectorIndex>& p_index, ListInfo* p_info, ValueType* vector)
            {
                ValueType* headVector = (ValueType*)p_index->GetSample((SizeType)(p_info - m_listInfos.data()));
                COMMON::SIMDUtils::ComputeSum(vector, headVector, m_iDataDimension);
            }

            inline void ParseEncoding(std::shared_ptr<VectorIndex>& p_index, ListInfo* p_info, ValueType* vector) { }

            void SelectPostingOffset(
                const std::vector<size_t>& p_postingListBytes,
                std::unique_ptr<int[]>& p_postPageNum,
                std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
                std::vector<int>& p_postingOrderInIndex)
            {
                p_postPageNum.reset(new int[p_postingListBytes.size()]);
                p_postPageOffset.reset(new std::uint16_t[p_postingListBytes.size()]);

                struct PageModWithID
                {
                    int id;

                    std::uint16_t rest;
                };

                struct PageModeWithIDCmp
                {
                    bool operator()(const PageModWithID& a, const PageModWithID& b) const
                    {
                        return a.rest == b.rest ? a.id < b.id : a.rest > b.rest;
                    }
                };

                std::set<PageModWithID, PageModeWithIDCmp> listRestSize;

                p_postingOrderInIndex.clear();
                p_postingOrderInIndex.reserve(p_postingListBytes.size());

                PageModWithID listInfo;
                for (size_t i = 0; i < p_postingListBytes.size(); ++i)
                {
                    if (p_postingListBytes[i] == 0)
                    {
                        continue;
                    }

                    listInfo.id = static_cast<int>(i);
                    listInfo.rest = static_cast<std::uint16_t>(p_postingListBytes[i] % PageSize);

                    listRestSize.insert(listInfo);
                }

                listInfo.id = -1;

                int currPageNum = 0;
                std::uint16_t currOffset = 0;

                while (!listRestSize.empty())
                {
                    listInfo.rest = PageSize - currOffset;
                    auto iter = listRestSize.lower_bound(listInfo); // avoid page-crossing
                    if (iter == listRestSize.end() || (listInfo.rest != PageSize && iter->rest == 0))
                    {
                        ++currPageNum;
                        currOffset = 0;
                    }
                    else
                    {
                        p_postPageNum[iter->id] = currPageNum;
                        p_postPageOffset[iter->id] = currOffset;

                        p_postingOrderInIndex.push_back(iter->id);

                        currOffset += iter->rest;
                        if (currOffset > PageSize)
                        {
                            LOG(Helper::LogLevel::LL_Error, "Crossing extra pages\n");
                            throw std::runtime_error("Read too many pages");
                        }

                        if (currOffset == PageSize)
                        {
                            ++currPageNum;
                            currOffset = 0;
                        }

                        currPageNum += static_cast<int>(p_postingListBytes[iter->id] / PageSize);

                        listRestSize.erase(iter);
                    }
                }

                LOG(Helper::LogLevel::LL_Info, "TotalPageNumbers: %d, IndexSize: %llu\n", currPageNum, static_cast<uint64_t>(currPageNum) * PageSize + currOffset);
            }

            void OutputSSDIndexFile(const std::string& p_outputFile,
                bool m_enableDeltaEncoding,
                bool m_enablePostingListRearrange,
                bool m_enableDataCompression,
                bool m_enableDictTraining,
                size_t p_spacePerVector,
                const std::vector<int>& p_postingListSizes,
                const std::vector<size_t>& p_postingListBytes,
                std::shared_ptr<VectorIndex> p_headIndex,
                Selection& p_postingSelections,
                const std::unique_ptr<int[]>& p_postPageNum,
                const std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
                const std::vector<int>& p_postingOrderInIndex,
                std::shared_ptr<VectorSet> p_fullVectors,
                size_t p_postingListOffset)
            {
                LOG(Helper::LogLevel::LL_Info, "Start output...\n");

                auto t1 = std::chrono::high_resolution_clock::now();

                auto ptr = SPTAG::f_createIO();
                int retry = 3;
                // open file 
                while (retry > 0 && (ptr == nullptr || !ptr->Initialize(p_outputFile.c_str(), std::ios::binary | std::ios::out)))
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed open file %s, retrying...\n", p_outputFile.c_str());
                    retry--;
                }

                if (ptr == nullptr || !ptr->Initialize(p_outputFile.c_str(), std::ios::binary | std::ios::out)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", p_outputFile.c_str());
                    throw std::runtime_error("Failed to open file for SSD index save");
                }
                // meta size of global info
                std::uint64_t listOffset = sizeof(int) * 4;
                // meta size of the posting lists
                listOffset += (sizeof(int) + sizeof(std::uint16_t) + sizeof(int) + sizeof(std::uint16_t)) * p_postingListSizes.size();
                // write listTotalBytes only when enabled data compression
                if (m_enableDataCompression)
                {
                    listOffset += sizeof(size_t) * p_postingListSizes.size();
                }

                // compression dict
                if (m_enableDataCompression && m_enableDictTraining)
                {
                    listOffset += sizeof(size_t);
                    listOffset += m_pCompressor->GetDictBuffer().size();
                }

                std::unique_ptr<char[]> paddingVals(new char[PageSize]);
                memset(paddingVals.get(), 0, sizeof(char) * PageSize);
                // paddingSize: bytes left in the last page
                std::uint64_t paddingSize = PageSize - (listOffset % PageSize);
                if (paddingSize == PageSize)
                {
                    paddingSize = 0;
                }
                else
                {
                    listOffset += paddingSize;
                }

                // Number of posting lists
                int i32Val = static_cast<int>(p_postingListSizes.size());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Number of vectors
                i32Val = static_cast<int>(p_fullVectors->Count());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Vector dimension
                i32Val = static_cast<int>(p_fullVectors->Dimension());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Page offset of list content section
                i32Val = static_cast<int>(listOffset / PageSize);
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Meta of each posting list
                for (int i = 0; i < p_postingListSizes.size(); ++i)
                {
                    size_t postingListByte = 0;
                    int pageNum = 0; // starting page number
                    std::uint16_t pageOffset = 0;
                    int listEleCount = 0;
                    std::uint16_t listPageCount = 0;

                    if (p_postingListSizes[i] > 0)
                    {
                        pageNum = p_postPageNum[i];
                        pageOffset = static_cast<std::uint16_t>(p_postPageOffset[i]);
                        listEleCount = static_cast<int>(p_postingListSizes[i]);
                        postingListByte = p_postingListBytes[i];
                        listPageCount = static_cast<std::uint16_t>(postingListByte / PageSize);
                        if (0 != (postingListByte % PageSize))
                        {
                            ++listPageCount;
                        }
                    }
                    // Total bytes of the posting list, write only when enabled data compression
                    if (m_enableDataCompression && ptr->WriteBinary(sizeof(postingListByte), reinterpret_cast<char*>(&postingListByte)) != sizeof(postingListByte)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page number of the posting list
                    if (ptr->WriteBinary(sizeof(pageNum), reinterpret_cast<char*>(&pageNum)) != sizeof(pageNum)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page offset
                    if (ptr->WriteBinary(sizeof(pageOffset), reinterpret_cast<char*>(&pageOffset)) != sizeof(pageOffset)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Number of vectors in the posting list
                    if (ptr->WriteBinary(sizeof(listEleCount), reinterpret_cast<char*>(&listEleCount)) != sizeof(listEleCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page count of the posting list
                    if (ptr->WriteBinary(sizeof(listPageCount), reinterpret_cast<char*>(&listPageCount)) != sizeof(listPageCount)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }
                // compression dict
                if (m_enableDataCompression && m_enableDictTraining)
                {
                    std::string dictBuffer = m_pCompressor->GetDictBuffer();
                    // dict size
                    size_t dictBufferSize = dictBuffer.size();
                    if (ptr->WriteBinary(sizeof(size_t), reinterpret_cast<char *>(&dictBufferSize)) != sizeof(dictBufferSize))
                    {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // dict
                    if (ptr->WriteBinary(dictBuffer.size(), const_cast<char *>(dictBuffer.data())) != dictBuffer.size())
                    {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                // Write padding vals
                if (paddingSize > 0)
                {
                    if (ptr->WriteBinary(paddingSize, reinterpret_cast<char*>(paddingVals.get())) != paddingSize) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                if (static_cast<uint64_t>(ptr->TellP()) != listOffset)
                {
                    LOG(Helper::LogLevel::LL_Info, "List offset not match!\n");
                    throw std::runtime_error("List offset mismatch");
                }

                LOG(Helper::LogLevel::LL_Info, "SubIndex Size: %llu bytes, %llu MBytes\n", listOffset, listOffset >> 20);

                listOffset = 0;

                std::uint64_t paddedSize = 0;
                // iterate over all the posting lists
                for (auto id : p_postingOrderInIndex)
                {
                    std::uint64_t targetOffset = static_cast<uint64_t>(p_postPageNum[id]) * PageSize + p_postPageOffset[id];
                    if (targetOffset < listOffset)
                    {
                        LOG(Helper::LogLevel::LL_Info, "List offset not match, targetOffset < listOffset!\n");
                        throw std::runtime_error("List offset mismatch");
                    }
                    // write padding vals before the posting list
                    if (targetOffset > listOffset)
                    {
                        if (targetOffset - listOffset > PageSize)
                        {
                            LOG(Helper::LogLevel::LL_Error, "Padding size greater than page size!\n");
                            throw std::runtime_error("Padding size mismatch with page size");
                        }

                        if (ptr->WriteBinary(targetOffset - listOffset, reinterpret_cast<char*>(paddingVals.get())) != targetOffset - listOffset) {
                            LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }

                        paddedSize += targetOffset - listOffset;

                        listOffset = targetOffset;
                    }

                    if (p_postingListSizes[id] == 0)
                    {
                        continue;
                    }
                    int postingListId = id + (int)p_postingListOffset;
                    // get posting list full content and write it at once
                    ValueType *headVector = nullptr;
                    if (m_enableDeltaEncoding)
                    {
                        headVector = (ValueType *)p_headIndex->GetSample(postingListId);
                    }
                    std::string postingListFullData = GetPostingListFullData(
                        postingListId, p_postingListSizes[id], p_postingSelections, p_fullVectors, m_enableDeltaEncoding, m_enablePostingListRearrange, headVector);
                    size_t postingListFullSize = p_postingListSizes[id] * p_spacePerVector;
                    if (postingListFullSize != postingListFullData.size())
                    {
                        LOG(Helper::LogLevel::LL_Error, "posting list full data size NOT MATCH! postingListFullData.size(): %zu postingListFullSize: %zu \n", postingListFullData.size(), postingListFullSize);
                        throw std::runtime_error("Posting list full size mismatch");
                    }
                    if (m_enableDataCompression)
                    {
                        std::string compressedData = m_pCompressor->Compress(postingListFullData, m_enableDictTraining);
                        size_t compressedSize = compressedData.size();
                        if (compressedSize != p_postingListBytes[id])
                        {
                            LOG(Helper::LogLevel::LL_Error, "Compressed size NOT MATCH! compressed size:%zu, pre-calculated compressed size:%zu\n", compressedSize, p_postingListBytes[id]);
                            throw std::runtime_error("Compression size mismatch");
                        }
                        if (ptr->WriteBinary(compressedSize, compressedData.data()) != compressedSize)
                        {
                            LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                        listOffset += compressedSize;
                    }
                    else
                    {
                        if (ptr->WriteBinary(postingListFullSize, postingListFullData.data()) != postingListFullSize)
                        {
                            LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                        listOffset += postingListFullSize;
                    }
                }

                paddingSize = PageSize - (listOffset % PageSize);
                if (paddingSize == PageSize)
                {
                    paddingSize = 0;
                }
                else
                {
                    listOffset += paddingSize;
                    paddedSize += paddingSize;
                }

                if (paddingSize > 0)
                {
                    if (ptr->WriteBinary(paddingSize, reinterpret_cast<char *>(paddingVals.get())) != paddingSize)
                    {
                        LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                LOG(Helper::LogLevel::LL_Info, "Padded Size: %llu, final total size: %llu.\n", paddedSize, listOffset);

                LOG(Helper::LogLevel::LL_Info, "Output done...\n");
                auto t2 = std::chrono::high_resolution_clock::now();
                LOG(Helper::LogLevel::LL_Info, "Time to write results:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()) / 1000);
            }

            void GetWritePosting(SizeType pid, std::string& posting, bool write = false) override {
                if (write) {
                    LOG(Helper::LogLevel::LL_Error, "Unsupport write\n");
                    exit(1);
                }
                ListInfo* listInfo = &(m_listInfos[pid]);
                size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
                size_t realBytes = listInfo->listEleCount * m_vectorInfoSize;
                posting.resize(totalBytes);
                int fileid = m_oneContext? 0: pid / m_listPerFile;
                Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
                auto numRead = indexFile->ReadBinary(totalBytes, posting.data(), listInfo->listOffset);
                if (numRead != totalBytes) {
                    LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                    throw std::runtime_error("File read mismatch");
                }
                char* ptr = (char*)(posting.c_str());
                memcpy(ptr, posting.c_str() + listInfo->pageOffset, realBytes);
                posting.resize(realBytes);
            }

        private:
            
            std::string m_extraFullGraphFile;

            std::vector<ListInfo> m_listInfos;
            bool m_oneContext;

            std::vector<std::shared_ptr<Helper::DiskIO>> m_indexFiles;
            bool m_postingMaskSidecarLoaded = false;
            bool m_postingMaskSidecarHasPageMasks = false;
            std::vector<std::uint64_t> m_postingMaskOffsets;
            std::vector<std::uint32_t> m_postingMaskVectorIDs;
            std::vector<std::uint16_t> m_postingMaskPageMasks;
            std::unique_ptr<Compressor> m_pCompressor;
            bool m_enableDeltaEncoding;
            bool m_enablePostingListRearrange;
            bool m_enableDataCompression;
            bool m_enableDictTraining;

            void (ExtraStaticSearcher<ValueType>::*m_parsePosting)(uint64_t&, uint64_t&, int, int);
            void (ExtraStaticSearcher<ValueType>::*m_parseEncoding)(std::shared_ptr<VectorIndex>&, ListInfo*, ValueType*);

            int m_vectorInfoSize = 0;
            int m_iDataDimension = 0;

            int m_totalListCount = 0;

            int m_listPerFile = 0;
        };
    } // namespace SPANN
} // namespace SPTAG

#endif // _SPTAG_SPANN_EXTRASTATICSEARCHER_H_
