#include "inc/Test.h"
#include "inc/Core/Common.h"
#include "inc/Core/SPANN/ExtraRocksDBController.h"

#include <rocksdb/utilities/options_util.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>

using namespace SPTAG;
using namespace SPTAG::SPANN;

BOOST_AUTO_TEST_SUITE(RocksDBBulkLoadTest)

BOOST_AUTO_TEST_CASE(BulkImportPreservesPostingsMasksAndNormalWritesAfterCheckpoint)
{
    const auto root = std::filesystem::temp_directory_path() / (
        "spfresh-bulk-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);
    const auto working = (root / "working").string();
    const auto checkpoint = (root / "checkpoint").string();
    constexpr size_t recordBytes = 32;
    constexpr SizeType recordsPerPosting = 2048;
    constexpr SizeType postingCount = 512;
    const auto posting = [](SizeType key) {
        std::string value(recordBytes * recordsPerPosting, 'x');
        for (SizeType record = 0; record < recordsPerPosting; ++record) {
            const SizeType id = key * recordsPerPosting + record;
            memcpy(value.data() + record * recordBytes, &id, sizeof(id));
            value[record * recordBytes + sizeof(id)] = 0;
        }
        return value;
    };
    {
        RocksDBIO db(working.c_str(), false, false, 16UL << 20, recordBytes);
        db.BeginBulkLoad();
        for (SizeType key = 0; key < postingCount; ++key) {
            BOOST_REQUIRE(db.Put(key, posting(key)) == ErrorCode::Success);
        }
        db.FinishBulkLoad();
        const SizeType appendedId = postingCount * recordsPerPosting;
        std::string appended(recordBytes, 'y');
        memcpy(appended.data(), &appendedId, sizeof(appendedId));
        appended[sizeof(appendedId)] = 0;
        BOOST_REQUIRE(db.Merge(0, appended) == ErrorCode::Success);
        db.Checkpoint(checkpoint);
    }
    rocksdb::DBOptions loadedOptions;
    std::vector<rocksdb::ColumnFamilyDescriptor> families;
    BOOST_REQUIRE(rocksdb::LoadLatestOptions(checkpoint, rocksdb::Env::Default(), &loadedOptions, &families).ok());
    BOOST_REQUIRE_EQUAL(families.size(), 1);
    const auto& options = families.front().options;
    BOOST_CHECK(!options.disable_auto_compactions);
    BOOST_CHECK(options.enable_blob_garbage_collection);
    BOOST_CHECK_EQUAL(options.level0_slowdown_writes_trigger, rocksdb::ColumnFamilyOptions().level0_slowdown_writes_trigger);
    BOOST_CHECK_EQUAL(options.level0_stop_writes_trigger, rocksdb::ColumnFamilyOptions().level0_stop_writes_trigger);
    {
        RocksDBIO db(checkpoint.c_str(), false, false, 16UL << 20, recordBytes);
        for (SizeType key = 0; key < postingCount; ++key) {
            std::string value;
            BOOST_REQUIRE(db.Get(key, &value) == ErrorCode::Success);
            const auto expected = posting(key);
            BOOST_REQUIRE_EQUAL(value.size(), expected.size() + (key == 0 ? recordBytes : 0));
            BOOST_CHECK_EQUAL(value.substr(0, expected.size()), expected);
            const auto ids = db.GetPostingIds(key);
            BOOST_REQUIRE_EQUAL(ids.size(), (recordsPerPosting + (key == 0 ? 1 : 0)) * sizeof(SizeType));
            SizeType finalId;
            memcpy(&finalId, ids.data() + ids.size() - sizeof(finalId), sizeof(finalId));
            BOOST_CHECK_EQUAL(finalId, key == 0 ? postingCount * recordsPerPosting : (key + 1) * recordsPerPosting - 1);
        }
    }
    std::ifstream log(working + "/LOG");
    const std::string contents((std::istreambuf_iterator<char>(log)), std::istreambuf_iterator<char>());
    size_t compactions = 0;
    for (size_t offset = 0; (offset = contents.find("\"event\": \"compaction_started\"", offset)) != std::string::npos; ++offset) {
        ++compactions;
    }
    BOOST_CHECK_GT(compactions, 0);
    BOOST_CHECK_LE(compactions, 4);
    std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_SUITE_END()
