#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/checkpoint.h>
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_EXTRAROCKSDBCONTROLLER_H_
#define _SPTAG_SPANN_EXTRAROCKSDBCONTROLLER_H_

#include "inc/Helper/KeyValueIO.h"
#include "inc/Helper/StringConvert.h"

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"

#include <map>
#include <cmath>
#include <climits>
#include <future>

namespace SPTAG::SPANN
{
    class RocksDBIO : public Helper::KeyValueIO
    {
        class AnnMergeOperator : public rocksdb::MergeOperator
        {
        public:
            bool FullMergeV2(const rocksdb::MergeOperator::MergeOperationInput& merge_in,
                rocksdb::MergeOperator::MergeOperationOutput* merge_out) const override
            {
                size_t length = (merge_in.existing_value)->size();
                for (const rocksdb::Slice& s : merge_in.operand_list) {
                    length += s.size();
                }
                (merge_out->new_value).resize(length);
                memcpy((char*)((merge_out->new_value).c_str()), 
                    (merge_in.existing_value)->data(), (merge_in.existing_value)->size());
                size_t start = (merge_in.existing_value)->size();
                for (const rocksdb::Slice& s : merge_in.operand_list) {
                    memcpy((char*)((merge_out->new_value).c_str() + start), s.data(), s.size());
                    start += s.size();
                }
                return true;
            }

            bool PartialMergeMulti(const rocksdb::Slice& key,
                const std::deque<rocksdb::Slice>& operand_list,
                std::string* new_value, rocksdb::Logger* logger) const override
            {
                size_t length = 0;
                for (const rocksdb::Slice& s : operand_list) {
                    length += s.size();
                }
                new_value->resize(length);
                size_t start = 0;
                for (const rocksdb::Slice& s : operand_list) {
                    memcpy((char*)(new_value->c_str() + start), s.data(), s.size());
                    start += s.size();
                }
                return true;
            }

            const char* Name() const override {
                return "AnnMergeOperator";
            }
        };

    public:
        RocksDBIO(const char* filePath, bool usdDirectIO, bool wal = false, std::uint64_t memoryBytes = 0, size_t postingRecordBytes = 0) {
            m_postingRecordBytes = postingRecordBytes;
            dbPath = std::string(filePath);
            //dbOptions.statistics = rocksdb::CreateDBStatistics();
            dbOptions.create_if_missing = true;
            if (!wal) {
                if (memoryBytes == 0) dbOptions.IncreaseParallelism();
                dbOptions.OptimizeLevelStyleCompaction();
                dbOptions.merge_operator.reset(new AnnMergeOperator);
                // dbOptions.statistics = rocksdb::CreateDBStatistics();

                // SST file size options
                dbOptions.target_file_size_base = 128UL * 1024 * 1024;
                dbOptions.target_file_size_multiplier = 2;
                dbOptions.max_bytes_for_level_base = 16 * 1024UL * 1024 * 1024;
                dbOptions.max_bytes_for_level_multiplier = 4;
                dbOptions.max_subcompactions = 16;
                dbOptions.num_levels = 4;
                dbOptions.level0_file_num_compaction_trigger = 1;
                dbOptions.level_compaction_dynamic_level_bytes = false;
                dbOptions.write_buffer_size = 16UL * 1024 * 1024;

                // rate limiter options
                // dbOptions.rate_limiter.reset(rocksdb::NewGenericRateLimiter(100UL << 20));

                // blob options
                dbOptions.enable_blob_files = true;
                dbOptions.min_blob_size = 64;
                dbOptions.blob_file_size = 8UL << 30;
                dbOptions.blob_compression_type = rocksdb::CompressionType::kNoCompression;
                dbOptions.enable_blob_garbage_collection = true;
                dbOptions.compaction_pri = rocksdb::CompactionPri::kRoundRobin;
                dbOptions.blob_garbage_collection_age_cutoff = 0.4;
                // dbOptions.blob_garbage_collection_force_threshold = 0.5;
                // dbOptions.blob_cache = rocksdb::NewLRUCache(5UL << 30);
                // dbOptions.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kFlushOnly;

                // dbOptions.env;
                // dbOptions.sst_file_manager = std::shared_ptr<rocksdb::SstFileManager>(rocksdb::NewSstFileManager(dbOptions.env));
                // dbOptions.sst_file_manager->SetStatisticsPtr(dbOptions.statistics);

                // compression options
                // dbOptions.compression = rocksdb::CompressionType::kLZ4Compression;
                // dbOptions.bottommost_compression = rocksdb::CompressionType::kZSTD;

                // block cache options
                rocksdb::BlockBasedTableOptions table_options;
                // table_options.block_cache = rocksdb::NewSimCache(rocksdb::NewLRUCache(1UL << 30), (8UL << 30), -1);
                table_options.block_cache = rocksdb::NewLRUCache(memoryBytes == 0 ? (3UL << 30) : std::min<std::uint64_t>(memoryBytes / 16, 64UL << 20), -1, true);
                // table_options.no_block_cache = true;

                // filter options
                table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
                table_options.optimize_filters_for_memory = true;

                if (memoryBytes != 0) {
                    dbOptions.max_background_jobs = 1;
                    dbOptions.max_subcompactions = 1;
                    dbOptions.write_buffer_size = std::max<std::uint64_t>(4096, std::min<std::uint64_t>(memoryBytes / 64, 4UL << 20));
                    dbOptions.max_write_buffer_number = 2;
                }
                dbOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
            }

            if (usdDirectIO) {
                dbOptions.use_direct_io_for_flush_and_compaction = true;
                dbOptions.use_direct_reads = true;
            }

            auto s = rocksdb::DB::Open(dbOptions, dbPath, &db);
            LOG(Helper::LogLevel::LL_Info, "SPFresh: New Rocksdb: %s\n", filePath);
            if (s != rocksdb::Status::OK()) {
                LOG(Helper::LogLevel::LL_Error, "\e[0;31mRocksdb Open Error\e[0m: %s\n", s.getState());
                if (memoryBytes != 0) throw std::runtime_error(s.ToString());
            }
        }

        void BeginBulkLoad() {
            auto status = db->SetOptions({
                {"disable_auto_compactions", "true"},
                {"enable_blob_garbage_collection", "false"},
                {"level0_slowdown_writes_trigger", std::to_string(INT_MAX - 1)},
                {"level0_stop_writes_trigger", std::to_string(INT_MAX)},
            });
            if (!status.ok()) throw std::runtime_error("SPFresh bulk import setup failed: " + status.ToString());
        }

        void FinishBulkLoad() {
            rocksdb::FlushOptions flush;
            flush.wait = true;
            auto status = db->Flush(flush);
            if (!status.ok()) throw std::runtime_error("SPFresh bulk import flush failed: " + status.ToString());
            status = db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
            if (!status.ok()) throw std::runtime_error("SPFresh bulk import compaction failed: " + status.ToString());
            status = db->SetOptions({
                {"enable_blob_garbage_collection", dbOptions.enable_blob_garbage_collection ? "true" : "false"},
                {"level0_slowdown_writes_trigger", std::to_string(dbOptions.level0_slowdown_writes_trigger)},
                {"level0_stop_writes_trigger", std::to_string(dbOptions.level0_stop_writes_trigger)},
            });
            if (!status.ok()) throw std::runtime_error("SPFresh bulk import option restore failed: " + status.ToString());
            status = db->EnableAutoCompaction({db->DefaultColumnFamily()});
            if (!status.ok()) throw std::runtime_error("SPFresh bulk import compaction restore failed: " + status.ToString());
        }

        void Checkpoint(const std::string& path) {
            auto status = db->FlushWAL(true);
            if (!status.ok()) throw std::runtime_error(status.ToString());
            rocksdb::Checkpoint* checkpoint = nullptr;
            status = rocksdb::Checkpoint::Create(db, &checkpoint);
            if (!status.ok()) throw std::runtime_error(status.ToString());
            std::unique_ptr<rocksdb::Checkpoint> owned(checkpoint);
            status = owned->CreateCheckpoint(path);
            if (!status.ok()) throw std::runtime_error(status.ToString());
        }

        ~RocksDBIO() override {
            /*
            std::string stats;
            db->GetProperty("rocksdb.stats", &stats);
            LOG(Helper::LogLevel::LL_Info, "RocksDB Status: %s\n%s", dbPath.c_str(),stats.c_str());
            */

            if (db) {
                ShutDown();
	    }
        }

        void ShutDown() override {
            db->Close();
            //DestroyDB(dbPath, dbOptions);
            delete db;
	    db = nullptr;
        }

        ErrorCode Get(const std::string& key, std::string* value) override {
            auto s = db->Get(rocksdb::ReadOptions(), key, value);
            if (s == rocksdb::Status::OK()) {
                return ErrorCode::Success;
            }
            else {
                LOG(Helper::LogLevel::LL_Error, "\e[0;31mError in Get\e[0m: %s, key: %d\n", s.getState(), *((SizeType*)(key.data())));
                return ErrorCode::Fail;
            }
        }

        ErrorCode Get(SizeType key, std::string* value) override {
            std::string k((char*)&key, sizeof(SizeType));
            return Get(k, value);
        }

        ErrorCode MultiGet(const std::vector<std::string>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            size_t num_keys = keys.size();

            rocksdb::Slice* slice_keys = new rocksdb::Slice[num_keys];
            rocksdb::PinnableSlice* slice_values = new rocksdb::PinnableSlice[num_keys];
            rocksdb::Status* statuses = new rocksdb::Status[num_keys];

            for (int i = 0; i < num_keys; i++) {
                slice_keys[i] = rocksdb::Slice(keys[i]);
            }

            db->MultiGet(rocksdb::ReadOptions(), db->DefaultColumnFamily(),
                num_keys, slice_keys, slice_values, statuses);

            for (int i = 0; i < num_keys; i++) {
                if (statuses[i] != rocksdb::Status::OK()) {
                    delete[] slice_keys;
                    delete[] slice_values;
                    delete[] statuses;
                    LOG(Helper::LogLevel::LL_Error, "\e[0;31mError in MultiGet\e[0m: %s, key: %d\n", statuses[i].getState(), *((SizeType*)(keys[i].data())));
                    return ErrorCode::Fail;
                }
                values->push_back(slice_values[i].ToString());
            }

            delete[] slice_keys;
            delete[] slice_values;
            delete[] statuses;
            return ErrorCode::Success;
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            std::vector<std::string> str_keys;

            for (const auto& key : keys) {
                str_keys.emplace_back((char*)(&key), sizeof(SizeType));
            }

            return MultiGet(str_keys, values, timeout);
        }

        ErrorCode Put(const std::string& key, const std::string& value) override {
            rocksdb::WriteBatch batch;
            batch.Put(key, value);
            if (m_postingRecordBytes != 0 && key.size() == sizeof(SizeType)) batch.Put("mask" + key, PostingIds(value));
            auto s = db->Write(rocksdb::WriteOptions(), &batch);
            if (s == rocksdb::Status::OK()) {
                return ErrorCode::Success;
            }
            else {
                LOG(Helper::LogLevel::LL_Error, "\e[0;31mError in Put\e[0m: %s, key: %d\n", s.getState(), *((SizeType*)(key.data())));
                return ErrorCode::Fail;
            }
        }

        ErrorCode Put(SizeType key, const std::string& value) override {
            std::string k((char*)&key, sizeof(SizeType));
            return Put(k, value);
        }

        ErrorCode Merge(SizeType key, const std::string& value) {
            if (value.empty()) {
                LOG(Helper::LogLevel::LL_Error, "Error! empty append posting!\n");
            }
            std::string k((char*)&key, sizeof(SizeType));
            rocksdb::WriteBatch batch;
            batch.Merge(k, value);
            if (m_postingRecordBytes != 0) batch.Merge("mask" + k, PostingIds(value));
            auto s = db->Write(rocksdb::WriteOptions(), &batch);
            if (s == rocksdb::Status::OK()) {
                return ErrorCode::Success;
            }
            else {
                LOG(Helper::LogLevel::LL_Error, "\e[0;31mError in Merge\e[0m: %s, key: %d\n", s.getState(), key);
                return ErrorCode::Fail;
            }
        }

        std::string GetPostingIds(SizeType key) const {
            std::string ids;
            auto status = db->Get(rocksdb::ReadOptions(), "mask" + std::string(reinterpret_cast<const char*>(&key), sizeof(key)), &ids);
            if (!status.ok()) throw std::runtime_error("SPFresh posting mask read failed: " + status.ToString());
            return ids;
        }

        ErrorCode Delete(SizeType key) override {
            std::string k((char*)&key, sizeof(SizeType));
            auto s = db->Delete(rocksdb::WriteOptions(), k);
            if (s == rocksdb::Status::OK()) {
                return ErrorCode::Success;
            }
            else {
                return ErrorCode::Fail;
            }
        }

        void ForceCompaction() {
            /*
            std::string stats;
            db->GetProperty("rocksdb.stats", &stats);
            LOG(Helper::LogLevel::LL_Info, "RocksDB Status:\n%s", stats.c_str());
            */
            LOG(Helper::LogLevel::LL_Info, "Start Compaction\n");
            auto s = db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
            LOG(Helper::LogLevel::LL_Info, "Finish Compaction\n");

            if (s != rocksdb::Status::OK()) {
                LOG(Helper::LogLevel::LL_Error, "\e[0;31mRocksdb Compact Error\e[0m: %s\n", s.getState());
            }
            /*
            db->GetProperty("rocksdb.stats", &stats);
            LOG(Helper::LogLevel::LL_Info, "RocksDB Status:\n%s", stats.c_str());
            */
        }

        void GetStat() {
            if (dbOptions.statistics != nullptr)
                LOG(Helper::LogLevel::LL_Info, "RocksDB statistics:\n %s\n", dbOptions.statistics->ToString().c_str());
            else
                LOG(Helper::LogLevel::LL_Info, "DB statistics not set!\n");
        }

    private:
        std::string PostingIds(const std::string& posting) const {
            if (posting.size() % m_postingRecordBytes != 0) throw std::runtime_error("invalid mutable posting record size");
            std::string ids;
            ids.reserve(posting.size() / m_postingRecordBytes * sizeof(SizeType));
            for (size_t offset = 0; offset < posting.size(); offset += m_postingRecordBytes) ids.append(posting.data() + offset, sizeof(SizeType));
            return ids;
        }
        size_t m_postingRecordBytes = 0;
        std::string dbPath;
        rocksdb::DB* db{};
        rocksdb::Options dbOptions;
    };
}
#endif // _SPTAG_SPANN_EXTRAROCKSDBCONTROLLER_H_
